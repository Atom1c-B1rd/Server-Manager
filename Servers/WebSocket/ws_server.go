package wsengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"server-manager/Data"
	"server-manager/Runtime"
)

type WSServer struct {
	cfg      *data.ServerConfig
	srv      *http.Server
	upgrader websocket.Upgrader
	mu       sync.Mutex
	running  bool
}
type wsHandler struct {
	match   string
	routeID string
	name    string
	fn      reflect.Value
}

func NewWSServer(cfg *data.ServerConfig) *WSServer {
	return &WSServer{cfg: cfg, upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}}
}
func (s *WSServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("WebSocket server already running")
	}
	s.mu.Unlock()
	field := strings.TrimSpace(s.cfg.MessageTypeField)
	if field == "" {
		field = "type"
	}
	handlers := []wsHandler{}
	for _, h := range s.cfg.Handlers {
		if strings.TrimSpace(h.Code) == "" {
			continue
		}
		i := scriptrt.NewInterpreter(s.cfg)
		_ = i.Use(map[string]map[string]reflect.Value{"github.com/gorilla/websocket/websocket": {"Conn": reflect.ValueOf((*websocket.Conn)(nil))}})
		v, e := scriptrt.EvalServerHandler(i, s.cfg, h.Code, h.Name)
		if e != nil || !v.IsValid() {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Error resolving WS handler %s: %v", h.Name, e))
			continue
		}
		if v.Kind() != reflect.Func || v.Type().NumIn() != 3 || v.Type().NumOut() > 1 || (v.Type().NumOut() == 1 && !v.Type().Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem())) {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("WS handler %s must accept (conn, messageType, payload) and return nothing or error", h.Name))
			continue
		}
		handlers = append(handlers, wsHandler{match: strings.TrimSpace(h.Path), routeID: strings.TrimSpace(h.RouteID), name: h.Name, fn: v})
		data.AppendLog(s.cfg.ID, fmt.Sprintf("Loaded WS message handler %s for type %q", h.Name, h.Path))
	}
	routes := s.cfg.Routes
	if len(routes) == 0 {
		routes = []data.RouteConfig{{ID: "r_default", Path: "/"}}
	}
	mux := http.NewServeMux()
	registeredRoutes := map[string]bool{}
	for _, route := range routes {
		path := strings.TrimSpace(route.Path)
		if path == "" {
			path = "/"
		}
		routePath := path
		routeID := strings.TrimSpace(route.ID)
		if registeredRoutes[routePath] {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Duplicate WebSocket route ignored: %s", routePath))
			continue
		}
		registeredRoutes[routePath] = true
		mux.HandleFunc(routePath, func(w http.ResponseWriter, r *http.Request) {
			conn, e := s.upgrader.Upgrade(w, r, nil)
			if e != nil {
				data.AppendLog(s.cfg.ID, fmt.Sprintf("WS Upgrade error on %s: %v", routePath, e))
				return
			}
			defer conn.Close()
			data.AppendLog(s.cfg.ID, fmt.Sprintf("WS client connected route_id=%s path=%s remote=%s", routeID, routePath, r.RemoteAddr))
			for {
				mt, raw, e := conn.ReadMessage()
				if e != nil {
					data.AppendLog(s.cfg.ID, fmt.Sprintf("WS client disconnected route_id=%s path=%s", routeID, routePath))
					return
				}
				var msg map[string]json.RawMessage
				typ := ""
				payload := raw
				if json.Unmarshal(raw, &msg) == nil {
					_ = json.Unmarshal(msg[field], &typ)
					if d, ok := msg["data"]; ok {
						payload = append([]byte(nil), d...)
					}
				}
				data.AppendLog(s.cfg.ID, fmt.Sprintf("WS message route_id=%s path=%s type=%q message=%q", routeID, routePath, typ, string(raw)))
				matched := false
				var selected *wsHandler
				for i := range handlers {
					if (handlers[i].routeID == "" || handlers[i].routeID == routeID) && typ != "" && handlers[i].match == typ {
						selected = &handlers[i]
						break
					}
				}
				if selected == nil {
					for i := range handlers {
						if (handlers[i].routeID == "" || handlers[i].routeID == routeID) && handlers[i].match == "*" {
							selected = &handlers[i]
							break
						}
					}
				}
				if selected != nil {
					h := *selected
					matched = true
					func() {
						defer func() {
							if rec := recover(); rec != nil {
								data.AppendLog(s.cfg.ID, fmt.Sprintf("WS handler %s panic: %v", h.name, rec))
							}
						}()
						args := []reflect.Value{reflect.ValueOf(conn), reflect.ValueOf(mt), reflect.ValueOf(payload)}
						out := h.fn.Call(args)
						if len(out) > 0 && !out[0].IsNil() {
							if err, ok := out[0].Interface().(error); ok && err != nil {
								data.AppendLog(s.cfg.ID, fmt.Sprintf("WS handler %s error: %v", h.name, err))
							}
						}
					}()
				}
				if !matched {
					_ = conn.WriteMessage(mt, append([]byte("Echo: "), raw...))
				}
			}
		})
	}
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	srv := &http.Server{Addr: addr, Handler: mux}
	s.mu.Lock()
	s.srv = srv
	s.running = true
	s.mu.Unlock()
	data.AppendLog(s.cfg.ID, fmt.Sprintf("WebSocket server started on port %d; routes=%d; message type field=%q", s.cfg.Port, len(routes), field))
	go func() {
		if e := srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("WebSocket server error: %v", e))
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}
	}()
	return nil
}
func (s *WSServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running || s.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e := s.srv.Shutdown(ctx)
	s.running = false
	data.AppendLog(s.cfg.ID, "WebSocket server stopped")
	return e
}
func (s *WSServer) IsRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }
