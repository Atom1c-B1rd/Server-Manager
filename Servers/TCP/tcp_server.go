package tcpengine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"server-manager/Data"
	"server-manager/Runtime"
)

type TCPServer struct {
	cfg      *data.ServerConfig
	listener net.Listener
	mu       sync.Mutex
	running  bool
	quit     chan struct{}
}
type tcpHandler struct {
	match string
	fn    func(net.Conn, []byte)
	name  string
}

func NewTCPServer(cfg *data.ServerConfig) *TCPServer {
	return &TCPServer{cfg: cfg, quit: make(chan struct{})}
}
func (s *TCPServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("TCP server already running")
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Port))
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on TCP: %w", err)
	}
	s.listener = listener
	s.running = true
	s.quit = make(chan struct{})
	s.mu.Unlock()
	field := strings.TrimSpace(s.cfg.MessageTypeField)
	if field == "" {
		field = "type"
	}
	handlers := []tcpHandler{}
	for _, h := range s.cfg.Handlers {
		if strings.TrimSpace(h.Code) == "" {
			continue
		}
		i := scriptrt.NewInterpreter(s.cfg)
		v, e := scriptrt.EvalServerHandler(i, s.cfg, h.Code, h.Name)
		if e != nil || !v.IsValid() {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Error resolving TCP handler %s: %v", h.Name, e))
			continue
		}
		if fn, ok := v.Interface().(func(net.Conn, []byte)); ok {
			handlers = append(handlers, tcpHandler{match: strings.TrimSpace(h.Path), fn: fn, name: h.Name})
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Loaded TCP handler %s for message type %q", h.Name, h.Path))
		} else {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("TCP handler %s must have signature func(net.Conn, []byte)", h.Name))
		}
	}
	data.AppendLog(s.cfg.ID, fmt.Sprintf("TCP server started on port %d; newline-delimited JSON messages use field %q", s.cfg.Port, field))
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				select {
				case <-s.quit:
					return
				default:
					data.AppendLog(s.cfg.ID, fmt.Sprintf("TCP accept error: %v", e))
					continue
				}
			}
			go s.handleConnection(c, handlers, field)
		}
	}()
	return nil
}
func (s *TCPServer) handleConnection(conn net.Conn, handlers []tcpHandler, field string) {
	defer conn.Close()
	data.AppendLog(s.cfg.ID, "TCP client connected: "+conn.RemoteAddr().String())
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			line = bytesTrimLine(line)
			s.dispatch(conn, line, handlers, field)
		}
		if err != nil {
			if err != io.EOF {
				data.AppendLog(s.cfg.ID, fmt.Sprintf("TCP read error: %v", err))
			}
			return
		}
	}
}
func bytesTrimLine(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}
func (s *TCPServer) dispatch(conn net.Conn, raw []byte, handlers []tcpHandler, field string) {
	var msg map[string]json.RawMessage
	typ := ""
	payload := raw
	if json.Unmarshal(raw, &msg) == nil {
		_ = json.Unmarshal(msg[field], &typ)
		if d, ok := msg["data"]; ok {
			payload = append([]byte(nil), d...)
		}
	}
	data.AppendLog(s.cfg.ID, fmt.Sprintf("TCP message type=%q bytes=%d", typ, len(raw)))
	for _, h := range handlers {
		if h.match == typ && typ != "" {
			h.fn(conn, payload)
			return
		}
	}
	for _, h := range handlers {
		if h.match == "*" {
			h.fn(conn, payload)
			return
		}
	}
	// Preserve useful behavior for clients that send plain text / unmatched types.
	_, _ = conn.Write(append([]byte("TCP Echo: "), raw...))
	_, _ = conn.Write([]byte("\n"))
}
func (s *TCPServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return nil
	}
	close(s.quit)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.running = false
	data.AppendLog(s.cfg.ID, "TCP server stopped")
	return nil
}
func (s *TCPServer) IsRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }
