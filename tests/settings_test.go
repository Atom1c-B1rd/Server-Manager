package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"server-manager/Data"
	"server-manager/Managers"
)

// ---- a minimal in-memory database/sql driver so sdk.DB can be tested without a real DB

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) { return &fakeConn{dsn: name}, nil }

type fakeConn struct{ dsn string }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return &fakeStmt{c}, nil }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("no tx") }

type fakeStmt struct{ c *fakeConn }

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) { return &fakeRows{dsn: s.c.dsn}, nil }

type fakeRows struct {
	dsn  string
	done bool
}

func (r *fakeRows) Columns() []string { return []string{"dsn"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0], r.done = r.dsn, true
	return nil
}

var registerFake sync.Once

func useFakeDriver() { registerFake.Do(func() { sql.Register("smfake", fakeDriver{}) }) }

// ---- helpers

func httpGetBody(t *testing.T, url string) string {
	t.Helper()
	var lastErr error
	for i := 0; i < 40; i++ {
		resp, err := http.Get(url)
		if err == nil {
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			return string(b)
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("GET %s failed: %v", url, lastErr)
	return ""
}

func settingsOf(cfg *data.ServerConfig) managers.SettingsInput {
	return managers.SettingsInput{
		Name: cfg.Name, Port: cfg.Port, AutoStart: cfg.AutoStart,
		Env: cfg.Env, Databases: cfg.Databases, Dependencies: cfg.Dependencies,
	}
}

func saveHTTP(t *testing.T, id string, port int, code, handler string, mod func(*data.ServerConfig)) *data.ServerConfig {
	t.Helper()
	cfg := &data.ServerConfig{
		ID: id, Name: id, Proto: "http", Port: port, Status: "stopped",
		Handlers: []data.HandlerConfig{{ID: "h1", Name: handler, Method: "GET", Path: "/t", Code: code}},
	}
	if mod != nil {
		mod(cfg)
	}
	if err := data.SaveServerConfig(cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	return cfg
}

// ---- tests

func TestSettingsValidation(t *testing.T) {
	useFakeDriver()
	m := managers.NewManager()
	base := &data.ServerConfig{ID: "srv_val", Name: "val", Proto: "http", Port: 18100, Status: "stopped"}
	if err := data.SaveServerConfig(base); err != nil {
		t.Fatal(err)
	}
	defer data.DeleteServerConfig("srv_val")

	cases := []struct {
		name   string
		mutate func(*managers.SettingsInput)
		want   string
	}{
		{"empty name", func(s *managers.SettingsInput) { s.Name = "  " }, "nombre"},
		{"port 0", func(s *managers.SettingsInput) { s.Port = 0 }, "puerto"},
		{"port too big", func(s *managers.SettingsInput) { s.Port = 70000 }, "puerto"},
		{"bad env key", func(s *managers.SettingsInput) { s.Env = []data.EnvVar{{Key: "1BAD", Value: "x"}} }, "variable"},
		{"dup env key", func(s *managers.SettingsInput) { s.Env = []data.EnvVar{{Key: "A"}, {Key: "A"}} }, "repetida"},
		{"unknown driver", func(s *managers.SettingsInput) {
			s.Databases = []data.DBConfig{{Name: "x", Driver: "nope", DSN: "d"}}
		}, "driver"},
		{"db without dsn", func(s *managers.SettingsInput) {
			s.Databases = []data.DBConfig{{Name: "x", Driver: "smfake"}}
		}, "DSN"},
		{"bad db name", func(s *managers.SettingsInput) {
			s.Databases = []data.DBConfig{{Name: "a b", Driver: "smfake", DSN: "d"}}
		}, "nombre"},
		{"dep stdlib-like path", func(s *managers.SettingsInput) {
			s.Dependencies = []data.Dependency{{Path: "fmt"}}
		}, "inválida"},
		{"dep flag injection", func(s *managers.SettingsInput) {
			s.Dependencies = []data.Dependency{{Path: "-exec=evil/x"}}
		}, "inválida"},
		{"dep bad version", func(s *managers.SettingsInput) {
			s.Dependencies = []data.Dependency{{Path: "github.com/google/uuid", Version: "master; rm -rf"}}
		}, "versión"},
	}
	for _, c := range cases {
		in := settingsOf(base)
		c.mutate(&in)
		_, err := m.UpdateSettings("srv_val", in)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: expected error containing %q, got %v", c.name, c.want, err)
		}
	}

	// nothing invalid may have been persisted
	got, _ := data.LoadServerConfig("srv_val")
	if got.Name != "val" || got.Port != 18100 || len(got.Env) != 0 {
		t.Fatalf("config changed by rejected updates: %+v", got)
	}

	if _, err := m.UpdateSettings("../etc/passwd", settingsOf(base)); err == nil {
		t.Fatal("path traversal in server ID must be rejected")
	}
}

func TestSettingsRenamePersistsAndKeepsHandlers(t *testing.T) {
	m := managers.NewManager()
	cfg := saveHTTP(t, "srv_ren", 18101, `package main
import "net/http"
func H(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }`, "H", nil)
	defer data.DeleteServerConfig("srv_ren")

	in := settingsOf(cfg)
	in.Name = "  Nuevo Nombre "
	in.AutoStart = true
	in.Env = []data.EnvVar{{Key: "TOKEN", Value: "abc"}, {}} // blank row is ignored
	if _, err := m.UpdateSettings("srv_ren", in); err != nil {
		t.Fatal(err)
	}

	got, _ := data.LoadServerConfig("srv_ren")
	if got.Name != "Nuevo Nombre" || !got.AutoStart || len(got.Env) != 1 || got.Env[0].Value != "abc" {
		t.Fatalf("settings not persisted: %+v", got)
	}
	if len(got.Handlers) != 1 || got.Handlers[0].Name != "H" {
		t.Fatalf("handlers were lost: %+v", got.Handlers)
	}
}

func TestSettingsPortConflict(t *testing.T) {
	m := managers.NewManager()
	a := saveHTTP(t, "srv_pa", 18102, "", "H", nil)
	saveHTTP(t, "srv_pb", 18103, "", "H", nil)
	defer data.DeleteServerConfig("srv_pa")
	defer data.DeleteServerConfig("srv_pb")

	in := settingsOf(a)
	in.Port = 18103
	if _, err := m.UpdateSettings("srv_pa", in); err == nil || !strings.Contains(err.Error(), "ya lo usa") {
		t.Fatalf("expected port conflict, got %v", err)
	}
}

func TestEnvSDKAndDatabase(t *testing.T) {
	useFakeDriver()
	m := managers.NewManager()
	saveHTTP(t, "srv_sdk", 18104, `package main
import (
	"net/http"
	"os"
	"server-manager/sdk"
)
func Hello(w http.ResponseWriter, r *http.Request) {
	db, err := sdk.DB("main")
	if err != nil { http.Error(w, err.Error(), 500); return }
	var dsn string
	if err := db.QueryRow("select dsn").Scan(&dsn); err != nil { http.Error(w, err.Error(), 500); return }
	if _, err := sdk.DB("missing"); err == nil { http.Error(w, "expected error", 500); return }
	sdk.Logf("served %s", os.Getenv("GREETING"))
	w.Write([]byte(os.Getenv("GREETING") + "|" + dsn))
}`, "Hello", func(c *data.ServerConfig) {
		c.Env = []data.EnvVar{{Key: "GREETING", Value: "hola"}}
		c.Databases = []data.DBConfig{{Name: "main", Driver: "smfake", DSN: "fake://local/db"}}
	})
	defer data.DeleteServerConfig("srv_sdk")

	if err := m.StartServer("srv_sdk"); err != nil {
		t.Fatal(err)
	}
	defer m.StopServer("srv_sdk")

	if got := httpGetBody(t, "http://localhost:18104/t"); got != "hola|fake://local/db" {
		t.Fatalf("unexpected body: %q", got)
	}
	found := false
	for _, l := range data.GetLogs("srv_sdk") {
		if strings.Contains(l, "[script] served hola") {
			found = true
		}
	}
	if !found {
		t.Fatalf("sdk.Logf did not reach the server logs: %v", data.GetLogs("srv_sdk"))
	}
}

func TestRestartWhenRuntimeSettingsChange(t *testing.T) {
	m := managers.NewManager()
	cfg := saveHTTP(t, "srv_rst", 18105, `package main
import (
	"net/http"
	"os"
)
func H(w http.ResponseWriter, r *http.Request) { w.Write([]byte(os.Getenv("MODE"))) }`, "H", func(c *data.ServerConfig) {
		c.Env = []data.EnvVar{{Key: "MODE", Value: "uno"}}
	})
	defer data.DeleteServerConfig("srv_rst")

	if err := m.StartServer("srv_rst"); err != nil {
		t.Fatal(err)
	}
	defer m.StopServer("srv_rst")
	if got := httpGetBody(t, "http://localhost:18105/t"); got != "uno" {
		t.Fatalf("before: %q", got)
	}

	// only a rename: must NOT restart
	in := settingsOf(cfg)
	in.Name = "renombrado"
	res, err := m.UpdateSettings("srv_rst", in)
	if err != nil || res.Restarted {
		t.Fatalf("rename should not restart: res=%+v err=%v", res, err)
	}

	// env + port change: restarts on the new port with the new env
	in.Port = 18106
	in.Env = []data.EnvVar{{Key: "MODE", Value: "dos"}}
	res, err = m.UpdateSettings("srv_rst", in)
	if err != nil || !res.Restarted || res.RestartError != "" {
		t.Fatalf("expected clean restart: res=%+v err=%v", res, err)
	}
	if got := httpGetBody(t, "http://localhost:18106/t"); got != "dos" {
		t.Fatalf("after: %q", got)
	}
	if _, err := http.Get("http://localhost:18105/t"); err == nil {
		t.Fatal("old port should be closed after the restart")
	}
	if st := m.GetServerStatus("srv_rst"); st != "running" {
		t.Fatalf("status = %q", st)
	}
}

func TestDependencyLoadedFromServerGoPath(t *testing.T) {
	m := managers.NewManager()
	saveHTTP(t, "srv_dep", 18107, `package main
import (
	"net/http"
	"example.com/hello"
)
func H(w http.ResponseWriter, r *http.Request) { w.Write([]byte(hello.Greet("mundo"))) }`, "H", func(c *data.ServerConfig) {
		c.Dependencies = []data.Dependency{{Path: "example.com/hello", Version: "latest", Status: "installed"}}
	})
	defer data.DeleteServerConfig("srv_dep")

	root, _ := data.DepsDir("srv_dep")
	pkgDir := filepath.Join(root, "src", "example.com", "hello")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "package hello\n\nfunc Greet(n string) string { return \"hola \" + n }\n"
	if err := os.WriteFile(filepath.Join(pkgDir, "hello.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := m.StartServer("srv_dep"); err != nil {
		t.Fatal(err)
	}
	defer m.StopServer("srv_dep")
	if got := httpGetBody(t, "http://localhost:18107/t"); got != "hola mundo" {
		t.Fatalf("unexpected body: %q", fmt.Sprint(got))
	}
}

// Needs the `go` command and network access: SM_TEST_NETWORK=1 go test ./...
func TestInstallRealDependency(t *testing.T) {
	if os.Getenv("SM_TEST_NETWORK") != "1" {
		t.Skip("set SM_TEST_NETWORK=1 to run (downloads github.com/google/uuid)")
	}
	m := managers.NewManager()
	saveHTTP(t, "srv_net", 18108, `package main
import (
	"net/http"
	"github.com/google/uuid"
)
func H(w http.ResponseWriter, r *http.Request) { w.Write([]byte(uuid.NewString())) }`, "H", func(c *data.ServerConfig) {
		c.Dependencies = []data.Dependency{{Path: "github.com/google/uuid", Version: "v1.6.0"}}
	})
	defer data.DeleteServerConfig("srv_net")

	res := m.InstallDependencies("srv_net")
	if !res.OK {
		t.Fatalf("install failed: %s", res.Message)
	}
	cfg, _ := data.LoadServerConfig("srv_net")
	if cfg.Dependencies[0].Status != "installed" {
		t.Fatalf("status not recorded: %+v", cfg.Dependencies)
	}

	if err := m.StartServer("srv_net"); err != nil {
		t.Fatal(err)
	}
	defer m.StopServer("srv_net")
	if got := httpGetBody(t, "http://localhost:18108/t"); len(got) != 36 || strings.Count(got, "-") != 4 {
		t.Fatalf("not a UUID: %q", got)
	}
}

func TestInstallMissingDependencyReportsError(t *testing.T) {
	if os.Getenv("SM_TEST_NETWORK") != "1" {
		t.Skip("set SM_TEST_NETWORK=1 to run")
	}
	m := managers.NewManager()
	saveHTTP(t, "srv_bad", 18109, "", "H", func(c *data.ServerConfig) {
		c.Dependencies = []data.Dependency{{Path: "github.com/google/this-module-does-not-exist-xyz", Version: "latest"}}
	})
	defer data.DeleteServerConfig("srv_bad")

	res := m.InstallDependencies("srv_bad")
	if res.OK || res.Message == "" {
		t.Fatalf("expected a failure with a message, got %+v", res)
	}
	cfg, _ := data.LoadServerConfig("srv_bad")
	if cfg.Dependencies[0].Status != "error" || cfg.Dependencies[0].Message == "" {
		t.Fatalf("error not recorded: %+v", cfg.Dependencies)
	}
	t.Logf("message shown to the user: %s", res.Message)
}
