package udpengine

import (
	"bytes"
	"fmt"
	"net"
	"server-manager/Data"
	"server-manager/Runtime"
	"strings"
	"sync"
)

type UDPServer struct {
	cfg     *data.ServerConfig
	conn    *net.UDPConn
	mu      sync.Mutex
	running bool
	quit    chan struct{}
}
type udpHandler struct {
	match string
	fn    func(*net.UDPConn, *net.UDPAddr, []byte)
	name  string
}

func NewUDPServer(cfg *data.ServerConfig) *UDPServer {
	return &UDPServer{cfg: cfg, quit: make(chan struct{})}
}
func (s *UDPServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("UDP server already running")
	}
	addr, e := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.cfg.Port))
	if e != nil {
		s.mu.Unlock()
		return e
	}
	conn, e := net.ListenUDP("udp", addr)
	if e != nil {
		s.mu.Unlock()
		return e
	}
	s.conn = conn
	s.running = true
	s.quit = make(chan struct{})
	s.mu.Unlock()
	handlers := []udpHandler{}
	for _, h := range s.cfg.Handlers {
		if strings.TrimSpace(h.Code) == "" {
			continue
		}
		i := scriptrt.NewInterpreter(s.cfg)
		v, e := scriptrt.EvalServerHandler(i, s.cfg, h.Code, h.Name)
		if e != nil || !v.IsValid() {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Error resolving UDP handler %s: %v", h.Name, e))
			continue
		}
		if fn, ok := v.Interface().(func(*net.UDPConn, *net.UDPAddr, []byte)); ok {
			handlers = append(handlers, udpHandler{strings.TrimSpace(h.Path), fn, h.Name})
			data.AppendLog(s.cfg.ID, fmt.Sprintf("Loaded UDP handler %s for packet type %q", h.Name, h.Path))
		} else {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("UDP handler %s must have signature func(*net.UDPConn, *net.UDPAddr, []byte)", h.Name))
		}
	}
	data.AppendLog(s.cfg.ID, fmt.Sprintf("UDP server started on port %d; packet format: type\\n<binary payload>", s.cfg.Port))
	go func() {
		buf := make([]byte, 65507)
		for {
			n, a, e := conn.ReadFromUDP(buf)
			if e != nil {
				select {
				case <-s.quit:
					return
				default:
					data.AppendLog(s.cfg.ID, fmt.Sprintf("UDP read error: %v", e))
					continue
				}
			}
			packet := append([]byte(nil), buf[:n]...)
			cut := bytes.IndexByte(packet, '\n')
			typ := ""
			payload := packet
			if cut >= 0 {
				typ = strings.TrimSuffix(string(packet[:cut]), "\r")
				payload = append([]byte(nil), packet[cut+1:]...)
			}
			data.AppendLog(s.cfg.ID, fmt.Sprintf("UDP packet type=%q bytes=%d from=%s", typ, len(payload), a))
			matched := false
			for _, h := range handlers {
				if typ != "" && h.match == typ {
					h.fn(conn, a, payload)
					matched = true
					break
				}
			}
			if !matched {
				for _, h := range handlers {
					if h.match == "*" {
						h.fn(conn, a, payload)
						matched = true
						break
					}
				}
			}
			if !matched {
				_, _ = conn.WriteToUDP(append([]byte("UDP Echo: "), packet...), a)
			}
		}
	}()
	return nil
}
func (s *UDPServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return nil
	}
	close(s.quit)
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.running = false
	data.AppendLog(s.cfg.ID, "UDP server stopped")
	return nil
}
func (s *UDPServer) IsRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }
