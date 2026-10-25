package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"server-manager/Data"
	"server-manager/Managers"
	"server-manager/Runtime"

	"github.com/gorilla/websocket"
)

type Msg struct {
	Type   string          `json:"type"`
	Target string          `json:"target"`
	Data   json.RawMessage `json:"data"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	// Only accept WebSocket connections opened by the UI itself. Without this
	// any web page the user visits could talk to localhost:3000 and read or
	// change the servers (including database credentials).
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // not a browser (CLI tools, tests)
		}
		u, err := url.Parse(origin)
		return err == nil && u.Host == r.Host
	},
}

// The UI listens here, so no managed server can use this port.
const uiPort = 3000

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

var (
	clients   = make(map[*Client]bool)
	clientsMu sync.Mutex
)

func UI() {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		tmpl, err := template.ParseFiles("./UI/UI.html")
		if err != nil {
			http.Error(
				w,
				"No se pudo cargar la UI",
				http.StatusInternalServerError,
			)
			log.Println("Template:", err)
			return
		}

		if err := tmpl.Execute(w, nil); err != nil {
			log.Println("Template execute:", err)
		}
	})

	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "./UI/DOCS.html")
	})

	mux.HandleFunc("/in", handleWS)

	// Recover per request: a UI panic must never take down the manager process.
	safeMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("UI panic (%s %s): %v", r.Method, r.URL.Path, recovered)
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = fmt.Fprint(w, `<!doctype html><html lang="es"><meta charset="utf-8"><title>Error de UI</title><body><h2>La interfaz tuvo un error</h2><p>El programa sigue ejecutándose. La página se recargará automáticamente.</p><script>setTimeout(()=>location.reload(),2000)</script></body></html>`)
			}
		}()
		mux.ServeHTTP(w, r)
	})

	addr := fmt.Sprintf("localhost:%d", uiPort)
	log.Println("UI disponible en http://" + addr)
	// A transient UI listener error should not terminate the whole application.
	for {
		if err := http.ListenAndServe(addr, safeMux); err != nil {
			log.Printf("UI HTTP server error (se reintentará): %v", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade:", err)
		return
	}

	client := &Client{
		conn: conn,
		send: make(chan []byte, 256),
	}

	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, client)
		clientsMu.Unlock()
		conn.Close()
	}()

	go func() {
		for msg := range client.send {
			_ = conn.WriteMessage(websocket.TextMessage, msg)
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Println("WebSocket disconnected:", err)
			break
		}

		var msg Msg
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Println("JSON inválido:", err)
			continue
		}

		wsHandler(client, msg)
	}
}

func wsHandler(client *Client, msg Msg) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("UI WebSocket handler panic (%s): %v", msg.Type, recovered)
			sendToClient(client, "error", msg.Target, map[string]string{"message": fmt.Sprintf("Falló la operación %s: %v. El programa sigue funcionando; revisá el log para ver el detalle.", msg.Type, recovered)})
		}
	}()
	logged := string(msg.Data)
	if msg.Type == "update_server" || msg.Type == "test_db" {
		logged = "<omitido: puede contener credenciales>"
	}
	log.Printf(
		"WebSocket recibido | Type: %s | Target: %s | Data: %s",
		msg.Type,
		msg.Target,
		logged,
	)

	switch msg.Type {
	case "list_servers":
		broadcastServerList()
		sendMeta(client)

	case "update_server":
		if msg.Target == "" {
			break
		}
		var in managers.SettingsInput
		if err := json.Unmarshal(msg.Data, &in); err != nil {
			sendToClient(client, "error", msg.Target, map[string]string{"message": "configuración inválida"})
			break
		}
		if in.Port == uiPort {
			sendToClient(client, "error", msg.Target, map[string]string{
				"message": fmt.Sprintf("el puerto %d lo usa la UI de Server Manager", uiPort),
			})
			break
		}
		res, err := managers.GlobalManager.UpdateSettings(msg.Target, in)
		if err != nil {
			sendToClient(client, "error", msg.Target, map[string]string{"message": err.Error()})
			break
		}
		sendToClient(client, "settings_saved", msg.Target, res)
		broadcastServerList()

	case "test_db":
		var req struct {
			Name   string `json:"name"`
			Driver string `json:"driver"`
			DSN    string `json:"dsn"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			break
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			result := map[string]interface{}{"name": req.Name, "ok": true}
			if err := scriptrt.TestDB(ctx, req.Driver, req.DSN); err != nil {
				result["ok"], result["message"] = false, err.Error()
			}
			sendToClient(client, "db_test", msg.Target, result)
		}()

	case "install_deps":
		if msg.Target == "" {
			break
		}
		go func() {
			res := managers.GlobalManager.InstallDependencies(msg.Target)
			sendToClient(client, "deps_installed", msg.Target, res)
			broadcastServerList()
		}()

	case "create_server":
		var req struct {
			Name  string `json:"name"`
			Proto string `json:"proto"`
			Port  int    `json:"port"`
		}
		if err := json.Unmarshal(msg.Data, &req); err == nil {
			srvID := fmt.Sprintf("srv_%d", time.Now().UnixNano())
			cfg := &data.ServerConfig{
				ID: srvID, Name: req.Name, Proto: req.Proto, Port: req.Port,
				Status: "stopped", Handlers: defaultHandlers(req.Proto), Routes: defaultRoutes(req.Proto), MessageTypeField: "type", GlobalScript: defaultGlobalScript(req.Proto),
				CreatedAt: time.Now().Format(time.RFC3339),
				Env:       []data.EnvVar{}, Databases: []data.DBConfig{}, Dependencies: defaultDependencies(req.Proto),
			}
			if err := data.SaveServerConfig(cfg); err != nil {
				sendToClient(client, "error", srvID, map[string]string{"message": "No se pudo crear el servidor: " + err.Error()})
				break
			}
			// Prepare protocol dependencies automatically after creation, without blocking the UI socket.
			go func() {
				if len(cfg.Dependencies) > 0 {
					res := managers.GlobalManager.InstallDependencies(srvID)
					if !res.OK {
						data.AppendLog(srvID, "[deps] Instalación inicial fallida: "+res.Message)
						sendToClient(client, "error", srvID, map[string]string{"message": "Servidor creado, pero falló la instalación de dependencias base: " + res.Message})
						broadcastServerList()
						return
					}
				}
				sendToClient(client, "server_created", srvID, map[string]string{"message": "Servidor creado con handlers y dependencias base preparados."})
				broadcastServerList()
			}()
		}

	case "delete_server":
		if msg.Target != "" {
			_ = managers.GlobalManager.StopServer(msg.Target)
			_ = data.DeleteServerConfig(msg.Target)
			broadcastServerList()
		}

	case "start_server":
		if msg.Target != "" {
			err := managers.GlobalManager.StartServer(msg.Target)
			if err != nil {
				sendToClient(client, "error", msg.Target, map[string]string{"message": err.Error()})
			}
			broadcastServerList()
		}

	case "stop_server":
		if msg.Target != "" {
			_ = managers.GlobalManager.StopServer(msg.Target)
			broadcastServerList()
		}

	case "save_global_script":
		if msg.Target != "" {
			var req struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(msg.Data, &req); err == nil {
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil {
					cfg.GlobalScript = composeGlobalScript(cfg.Proto, cfg.GlobalScript, req.Code)
					if err := data.SaveServerConfig(cfg); err != nil {
						sendToClient(client, "error", msg.Target, map[string]string{"message": "No se pudo guardar el script global: " + err.Error()})
					} else {
						sendToClient(client, "global_script_saved", msg.Target, map[string]string{"message": "Script global guardado"})
						broadcastServerList()
					}
				}
			}
		}

	case "save_protocol_options":
		if msg.Target != "" {
			var req struct {
				MessageTypeField string `json:"message_type_field"`
			}
			if err := json.Unmarshal(msg.Data, &req); err == nil {
				field := strings.TrimSpace(req.MessageTypeField)
				if field == "" {
					field = "type"
				}
				wasRunning := managers.GlobalManager.GetServerStatus(msg.Target) == "running"
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil {
					cfg.MessageTypeField = field
					if err := data.SaveServerConfig(cfg); err != nil {
						sendToClient(client, "error", msg.Target, map[string]string{"message": err.Error()})
					} else {
						if wasRunning {
							_ = managers.GlobalManager.StopServer(msg.Target)
							if startErr := managers.GlobalManager.StartServer(msg.Target); startErr != nil {
								sendToClient(client, "error", msg.Target, map[string]string{"message": "Formato guardado, pero no se pudo reiniciar el servidor: " + startErr.Error()})
							}
						}
						broadcastServerList()
					}
				}
			}
		}

	case "save_route":
		if msg.Target != "" {
			var route data.RouteConfig
			if err := json.Unmarshal(msg.Data, &route); err == nil {
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil && cfg.Proto == "ws" {
					if route.ID == "" {
						route.ID = fmt.Sprintf("r_%d", time.Now().UnixNano())
					}
					if !strings.HasPrefix(route.Path, "/") {
						route.Path = "/" + route.Path
					}
					updated := false
					for i, old := range cfg.Routes {
						if old.ID == route.ID {
							cfg.Routes[i] = route
							updated = true
							break
						}
					}
					if !updated {
						cfg.Routes = append(cfg.Routes, route)
					}
					if err := data.SaveServerConfig(cfg); err != nil {
						sendToClient(client, "error", msg.Target, map[string]string{"message": err.Error()})
					} else {
						broadcastServerList()
					}
				}
			}
		}

	case "delete_route":
		if msg.Target != "" {
			var req struct {
				RouteID string `json:"route_id"`
			}
			if err := json.Unmarshal(msg.Data, &req); err == nil {
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil {
					filtered := cfg.Routes[:0]
					for _, route := range cfg.Routes {
						if route.ID != req.RouteID {
							filtered = append(filtered, route)
						}
					}
					cfg.Routes = filtered
					_ = data.SaveServerConfig(cfg)
					broadcastServerList()
				}
			}
		}

	case "save_handler":
		if msg.Target != "" {
			var h data.HandlerConfig
			if err := json.Unmarshal(msg.Data, &h); err == nil {
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil {
					// Check if updating existing or adding new
					updated := false
					for i, existing := range cfg.Handlers {
						if existing.ID == h.ID || (h.ID == "" && existing.Name == h.Name) {
							cfg.Handlers[i] = h
							updated = true
							break
						}
					}
					if !updated {
						if h.ID == "" {
							h.ID = fmt.Sprintf("h_%d", time.Now().UnixNano())
						}
						cfg.Handlers = append(cfg.Handlers, h)
					}
					_ = data.SaveServerConfig(cfg)
					broadcastServerList()
				}
			}
		}

	case "delete_handler":
		if msg.Target != "" {
			var req struct {
				HandlerID string `json:"handler_id"`
			}
			if err := json.Unmarshal(msg.Data, &req); err == nil {
				cfg, err := data.LoadServerConfig(msg.Target)
				if err == nil {
					var filtered []data.HandlerConfig
					for _, h := range cfg.Handlers {
						if h.ID != req.HandlerID {
							filtered = append(filtered, h)
						}
					}
					cfg.Handlers = filtered
					_ = data.SaveServerConfig(cfg)
					broadcastServerList()
				}
			}
		}

	case "get_logs":
		if msg.Target != "" {
			logs := data.GetLogs(msg.Target)
			sendToClient(client, "logs", msg.Target, logs)
		}
	}
}

// defaultGlobalScript contains the server-wide package declaration, imports and
// shared declarations. Route/handler editors intentionally contain functions only.
func defaultGlobalScript(proto string) string {
	// Standard-library imports available by default to every newly created server.
	// They are imported once in the generated global script, not in each handler.
	imports := []string{`"encoding/json"`, `"fmt"`, `sdk "server-manager/sdk"`}
	switch proto {
	case "http":
		imports = append(imports, `"net/http"`)
	case "ws":
		imports = append(imports, `"github.com/gorilla/websocket"`)
	case "tcp", "udp":
		imports = append(imports, `"net"`)
	}
	sort.Strings(imports)
	var b strings.Builder
	b.WriteString("package main\n\nimport (\n")
	for _, spec := range imports {
		b.WriteString("\t" + spec + "\n")
	}
	b.WriteString(")\n\n// SERVER_MANAGER_GLOBAL_DECLARATIONS")
	return b.String()
}

// composeGlobalScript keeps imports and generated infrastructure managed by the
// application, while exposing only declarations (types, vars and helper funcs)
// in the UI editor.
func composeGlobalScript(proto, existing, declarations string) string {
	imports := map[string]bool{}
	importLine := regexp.MustCompile(`(?m)^\s*(?:[A-Za-z_][A-Za-z0-9_]*\s+)?"[^"\n]+"\s*$`)
	group := regexp.MustCompile(`(?s)import\s*\((.*?)\)`)
	single := regexp.MustCompile(`(?m)^\s*import\s+((?:[A-Za-z_][A-Za-z0-9_]*\s+)?"[^"\n]+")\s*$`)
	for _, spec := range importLine.FindAllString(existing, -1) {
		imports[strings.TrimSpace(spec)] = true
	}
	for _, match := range single.FindAllStringSubmatch(existing, -1) {
		imports[strings.TrimSpace(match[1])] = true
	}
	for _, match := range group.FindAllStringSubmatch(existing, -1) {
		for _, line := range strings.Split(match[1], "\n") {
			line = strings.TrimSpace(strings.SplitN(line, "//", 2)[0])
			if line != "" && importLine.MatchString(line) {
				imports[line] = true
			}
		}
	}
	defaults := defaultGlobalScript(proto)
	for _, spec := range importLine.FindAllString(defaults, -1) {
		imports[strings.TrimSpace(spec)] = true
	}
	delete(imports, "package main")
	list := make([]string, 0, len(imports))
	for spec := range imports {
		list = append(list, spec)
	}
	sort.Strings(list)
	body := strings.TrimSpace(declarations)
	body = regexp.MustCompile(`(?m)^\s*package\s+[A-Za-z_][A-Za-z0-9_]*\s*\n?`).ReplaceAllString(body, "")
	body = single.ReplaceAllString(body, "")
	body = group.ReplaceAllString(body, "")
	var b strings.Builder
	b.WriteString("package main\n\nimport (\n")
	for _, spec := range list {
		b.WriteString("\t" + spec + "\n")
	}
	b.WriteString(")\n\n// SERVER_MANAGER_GLOBAL_DECLARATIONS")
	if body != "" {
		b.WriteString("\n\n" + body)
	}
	return b.String()
}

// defaultHandlers contain only the function being edited; imports and shared
// structures are kept in the server's GlobalScript.
func defaultHandlers(proto string) []data.HandlerConfig {
	var h data.HandlerConfig
	switch proto {
	case "http":
		h = data.HandlerConfig{ID: "h_default", Name: "HandleRequest", Method: "GET", Path: "/", Code: `func HandleRequest(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/plain; charset=utf-8")
    w.WriteHeader(http.StatusOK)
    _, _ = w.Write([]byte("Server Manager: HTTP server is running"))
}`}
	case "ws":
		h = data.HandlerConfig{ID: "h_default", Name: "HandleMessage", Path: "*", Code: `func HandleMessage(conn *websocket.Conn, msgType int, payload []byte) error {
    return conn.WriteMessage(msgType, append([]byte("Echo: "), payload...))
}`}
	case "tcp":
		h = data.HandlerConfig{ID: "h_default", Name: "HandleData", Path: "*", Code: `func HandleData(conn net.Conn, payload []byte) {
    reply := append([]byte("Echo: "), payload...)
    reply = append(reply, '\n')
    _, _ = conn.Write(reply)
}`}
	case "udp":
		h = data.HandlerConfig{ID: "h_default", Name: "HandleData", Path: "*", Code: `func HandleData(conn *net.UDPConn, addr *net.UDPAddr, payload []byte) {
    _, _ = conn.WriteToUDP(append([]byte("Echo: "), payload...), addr)
}`}
	default:
		return []data.HandlerConfig{}
	}
	return []data.HandlerConfig{h}
}

func defaultRoutes(proto string) []data.RouteConfig {
	if proto == "ws" {
		return []data.RouteConfig{{ID: "r_default", Path: "/"}}
	}
	return []data.RouteConfig{}
}

// defaultDependencies are protocol-specific third-party dependencies.
// Gorilla WebSocket is installed only for WebSocket servers.
func defaultDependencies(proto string) []data.Dependency {
	if proto == "ws" {
		return []data.Dependency{{Path: "github.com/gorilla/websocket", Version: "v1.5.3"}}
	}
	return []data.Dependency{}
}

// sendMeta tells the UI what this installation supports: the database drivers
// compiled in and whether the `go` command (needed to fetch dependencies) exists.
func sendMeta(client *Client) {
	_, goErr := exec.LookPath("go")
	sendToClient(client, "meta", "", map[string]interface{}{
		"drivers":      scriptrt.Drivers(),
		"go_available": goErr == nil,
		"sdk_import":   scriptrt.SDKImportPath,
	})
}

func broadcastServerList() {
	list, err := data.ListServerConfigs()
	if err != nil {
		return
	}

	for _, srv := range list {
		srv.Status = managers.GlobalManager.GetServerStatus(srv.ID)
	}

	raw, _ := json.Marshal(map[string]interface{}{
		"type": "server_list",
		"data": list,
	})

	clientsMu.Lock()
	defer clientsMu.Unlock()
	for c := range clients {
		select {
		case c.send <- raw:
		default:
		}
	}
}

func sendToClient(client *Client, msgType string, target string, payload interface{}) {
	dataRaw, _ := json.Marshal(payload)
	raw, _ := json.Marshal(Msg{
		Type:   msgType,
		Target: target,
		Data:   dataRaw,
	})

	select {
	case client.send <- raw:
	default:
	}
}
