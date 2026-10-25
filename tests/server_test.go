package tests

import (
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"server-manager/Data"
	"server-manager/Managers"

	"github.com/gorilla/websocket"
)

func TestMain(m *testing.M) {
	_ = os.RemoveAll("Data")
	os.Exit(m.Run())
}

func TestStorage(t *testing.T) {
	cfg := &data.ServerConfig{
		ID:     "test_srv_1",
		Name:   "Test HTTP",
		Proto:  "http",
		Port:   18080,
		Status: "stopped",
	}

	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("Failed to save server config: %v", err)
	}

	loaded, err := data.LoadServerConfig("test_srv_1")
	if err != nil {
		t.Fatalf("Failed to load server config: %v", err)
	}

	if loaded.Name != "Test HTTP" || loaded.Port != 18080 {
		t.Fatalf("Config mismatch: %+v", loaded)
	}

	list, err := data.ListServerConfigs()
	if err != nil || len(list) == 0 {
		t.Fatalf("Failed to list server configs: %v", err)
	}

	data.AppendLog("test_srv_1", "Test log message")
	logs := data.GetLogs("test_srv_1")
	if len(logs) == 0 || !strings.Contains(logs[0], "Test log message") {
		t.Fatalf("Logs mismatch: %v", logs)
	}

	_ = data.DeleteServerConfig("test_srv_1")
}

func TestHTTPServerLifecycle(t *testing.T) {
	cfg := &data.ServerConfig{
		ID:     "srv_http_test",
		Name:   "Dynamic HTTP Test",
		Proto:  "http",
		Port:   18081,
		Status: "stopped",
		Handlers: []data.HandlerConfig{
			{
				ID:     "h1",
				Name:   "HelloHandler",
				Method: "GET",
				Path:   "/hello",
				Code: `package main
import "net/http"

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Hello from dynamic HTTP Yaegi!"))
}`,
			},
		},
	}

	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	m := managers.NewManager()
	if err := m.StartServer("srv_http_test"); err != nil {
		t.Fatalf("Failed to start HTTP server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:18081/hello")
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Hello from dynamic HTTP Yaegi!") {
		t.Fatalf("Unexpected response body: %s", string(body))
	}

	if err := m.StopServer("srv_http_test"); err != nil {
		t.Fatalf("Failed to stop HTTP server: %v", err)
	}
}

func TestWSServerLifecycle(t *testing.T) {
	cfg := &data.ServerConfig{
		ID:     "srv_ws_test",
		Name:   "Dynamic WS Test",
		Proto:  "ws",
		Port:   18082,
		Status: "stopped",
		Handlers: []data.HandlerConfig{
			{
				ID:   "h1",
				Name: "EchoWS",
				Code: `package main
import "github.com/gorilla/websocket"

func EchoWS(conn *websocket.Conn, msgType int, payload []byte) error {
	return conn.WriteMessage(msgType, append([]byte("WS Echo: "), payload...))
}`,
			},
		},
	}

	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	m := managers.NewManager()
	if err := m.StartServer("srv_ws_test"); err != nil {
		t.Fatalf("Failed to start WS server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:18082/", nil)
	if err != nil {
		t.Fatalf("Failed to connect WS client: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("Ping WS")); err != nil {
		t.Fatalf("Failed to write WS msg: %v", err)
	}

	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read WS msg: %v", err)
	}

	if string(reply) != "WS Echo: Ping WS" {
		t.Fatalf("Unexpected WS reply: %s", string(reply))
	}

	_ = m.StopServer("srv_ws_test")
}

func TestTCPServerLifecycle(t *testing.T) {
	cfg := &data.ServerConfig{
		ID:     "srv_tcp_test",
		Name:   "Dynamic TCP Test",
		Proto:  "tcp",
		Port:   18083,
		Status: "stopped",
		Handlers: []data.HandlerConfig{
			{
				ID:   "h1",
				Name: "OnTCPData",
				Code: `package main
import "net"

func OnTCPData(conn net.Conn, data []byte) {
	conn.Write(append([]byte("TCP ACK: "), data...))
}`,
			},
		},
	}

	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	m := managers.NewManager()
	if err := m.StartServer("srv_tcp_test"); err != nil {
		t.Fatalf("Failed to start TCP server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:18083")
	if err != nil {
		t.Fatalf("Failed to connect TCP: %v", err)
	}
	defer conn.Close()

	_, _ = conn.Write([]byte("Hello TCP"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read TCP: %v", err)
	}

	if string(buf[:n]) != "TCP ACK: Hello TCP" {
		t.Fatalf("Unexpected TCP response: %s", string(buf[:n]))
	}

	_ = m.StopServer("srv_tcp_test")
}

func TestUDPServerLifecycle(t *testing.T) {
	cfg := &data.ServerConfig{
		ID:     "srv_udp_test",
		Name:   "Dynamic UDP Test",
		Proto:  "udp",
		Port:   18084,
		Status: "stopped",
		Handlers: []data.HandlerConfig{
			{
				ID:   "h1",
				Name: "OnUDPData",
				Code: `package main
import "net"

func OnUDPData(conn *net.UDPConn, addr *net.UDPAddr, data []byte) {
	conn.WriteToUDP(append([]byte("UDP ACK: "), data...), addr)
}`,
			},
		},
	}

	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	m := managers.NewManager()
	if err := m.StartServer("srv_udp_test"); err != nil {
		t.Fatalf("Failed to start UDP server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	rAddr, err := net.ResolveUDPAddr("udp", "localhost:18084")
	if err != nil {
		t.Fatalf("Resolve UDP error: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, rAddr)
	if err != nil {
		t.Fatalf("Dial UDP error: %v", err)
	}
	defer conn.Close()

	_, _ = conn.Write([]byte("Hello UDP"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read UDP error: %v", err)
	}

	if string(buf[:n]) != "UDP ACK: Hello UDP" {
		t.Fatalf("Unexpected UDP response: %s", string(buf[:n]))
	}

	_ = m.StopServer("srv_udp_test")
}
