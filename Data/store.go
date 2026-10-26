package data

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type RouteConfig struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

type HandlerConfig struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Method  string `json:"method"`             // GET, POST, PUT, DELETE for HTTP, or EVENT type (ON_DATA, ON_CONNECT, ON_MESSAGE)
	Path    string `json:"path"`               // route path for HTTP or topic/event for WS/TCP/UDP
	RouteID string `json:"route_id,omitempty"` // WebSocket route this message handler belongs to
	Code    string `json:"code"`               // Go code for Yaegi
}

// EnvVar is an environment variable visible to the handlers of one server
// (read with os.Getenv inside the handler code).
type EnvVar struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DBConfig is a named database connection. Handlers get a *sql.DB with
// sdk.DB(Name). Driver must be one compiled into the binary (see Runtime/drivers.go).
type DBConfig struct {
	Name   string `json:"name"`
	Driver string `json:"driver"` // "postgres", "mysql", ...
	DSN    string `json:"dsn"`
}

// Dependency is a third-party Go package the handlers can import.
type Dependency struct {
	Path    string `json:"path"`              // import path, e.g. github.com/google/uuid
	Version string `json:"version"`           // "latest" or a version such as v1.6.0
	Status  string `json:"status,omitempty"`  // "" (pending), "installed" or "error"
	Message string `json:"message,omitempty"` // last install error
}

type ServerConfig struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	Proto            string          `json:"proto"` // "tcp", "udp", "ws", "http"
	Port             int             `json:"port"`
	Status           string          `json:"status"` // "running", "stopped", "error"
	Handlers         []HandlerConfig `json:"handlers"`
	Routes           []RouteConfig   `json:"routes,omitempty"`
	MessageTypeField string          `json:"message_type_field,omitempty"`
	GlobalScript     string          `json:"global_script,omitempty"`
	CreatedAt        string          `json:"created_at"`
	AutoStart        bool            `json:"autostart,omitempty"`
	Env              []EnvVar        `json:"env,omitempty"`
	Databases        []DBConfig      `json:"databases,omitempty"`
	Dependencies     []Dependency    `json:"dependencies,omitempty"`
}

var (
	storeMu   sync.RWMutex
	logMu     sync.RWMutex
	logsCache = make(map[string][]string)

	serverDir = filepath.Join("Data", "Server")
	logsDir   = filepath.Join("Data", "Logs")
	depsDir   = filepath.Join("Data", "Deps")

	validIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// validID guards every path built from a server ID (it arrives from the UI).
func validID(id string) error {
	if !validIDRe.MatchString(id) {
		return fmt.Errorf("invalid server ID %q", id)
	}
	return nil
}

// DepsDir returns the absolute GOPATH-style directory holding the downloaded
// dependencies of a server (packages live under <dir>/src/<import path>).
func DepsDir(id string) (string, error) {
	if err := validID(id); err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(depsDir, id))
}

func EnsureDirs() {
	_ = os.MkdirAll(serverDir, 0755)
	_ = os.MkdirAll(logsDir, 0755)
}

func init() {
	EnsureDirs()
}

func SaveServerConfig(cfg *ServerConfig) error {
	storeMu.Lock()
	defer storeMu.Unlock()
	return saveLocked(cfg)
}

func saveLocked(cfg *ServerConfig) error {
	EnsureDirs()

	if cfg.ID == "" {
		return fmt.Errorf("server ID cannot be empty")
	}
	if err := validID(cfg.ID); err != nil {
		return err
	}

	if cfg.CreatedAt == "" {
		cfg.CreatedAt = time.Now().Format(time.RFC3339)
	}

	filePath := filepath.Join(serverDir, cfg.ID+".json")
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	// 0600: the file may hold database credentials
	return os.WriteFile(filePath, data, 0600)
}

func LoadServerConfig(id string) (*ServerConfig, error) {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return loadLocked(id)
}

func loadLocked(id string) (*ServerConfig, error) {
	if err := validID(id); err != nil {
		return nil, err
	}

	filePath := filepath.Join(serverDir, id+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg ServerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	normalizeScripts(&cfg)
	return &cfg, nil
}

// UpdateServerConfig performs an atomic read-modify-write of one server config.
// fn receives the current config and the configs of all the other servers; if
// it returns an error nothing is written.
func UpdateServerConfig(id string, fn func(cfg *ServerConfig, others []*ServerConfig) error) (*ServerConfig, error) {
	storeMu.Lock()
	defer storeMu.Unlock()

	cfg, err := loadLocked(id)
	if err != nil {
		return nil, err
	}

	all, _ := listLocked()
	others := make([]*ServerConfig, 0, len(all))
	for _, o := range all {
		if o.ID != id {
			others = append(others, o)
		}
	}

	if err := fn(cfg, others); err != nil {
		return nil, err
	}
	if err := saveLocked(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func ListServerConfigs() ([]*ServerConfig, error) {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return listLocked()
}

func listLocked() ([]*ServerConfig, error) {
	EnsureDirs()

	files, err := os.ReadDir(serverDir)
	if err != nil {
		return nil, err
	}

	var list []*ServerConfig
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".json" {
			filePath := filepath.Join(serverDir, f.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}
			var cfg ServerConfig
			if err := json.Unmarshal(data, &cfg); err == nil {
				normalizeScripts(&cfg)
				list = append(list, &cfg)
			}
		}
	}
	return list, nil
}

// normalizeScripts migrates older configurations that stored package/import
// declarations inside each handler into the server-wide script field.
func normalizeScripts(cfg *ServerConfig) {
	if cfg == nil {
		return
	}
	defaults := map[string][]string{
		"http": {`sdk "server-manager/sdk"`, `"net/http"`},
		"ws":   {`sdk "server-manager/sdk"`, `"github.com/gorilla/websocket"`},
		"tcp":  {`sdk "server-manager/sdk"`, `"net"`},
		"udp":  {`sdk "server-manager/sdk"`, `"net"`},
	}
	imports := map[string]bool{}
	var importLines []string
	for _, item := range defaults[cfg.Proto] {
		imports[item] = true
	}
	global := strings.TrimSpace(cfg.GlobalScript)
	if global != "" {
		for _, m := range regexp.MustCompile(`(?m)^\s*(?:[\w.]+\s+)?"[^"\n]+"\s*$`).FindAllString(global, -1) {
			v := strings.TrimSpace(m)
			if !imports[v] {
				imports[v] = true
			}
		}
	}
	for i := range cfg.Handlers {
		code := cfg.Handlers[i].Code
		if strings.TrimSpace(code) == "" {
			continue
		}
		for _, m := range regexp.MustCompile(`(?m)^\s*import\s+((?:[\w.]+\s+)?"[^"\n]+")\s*$`).FindAllStringSubmatch(code, -1) {
			v := strings.TrimSpace(m[1])
			if !imports[v] {
				imports[v] = true
			}
		}
		for _, block := range regexp.MustCompile(`(?s)import\s*\((.*?)\)`).FindAllStringSubmatch(code, -1) {
			for _, line := range strings.Split(block[1], "\n") {
				v := strings.TrimSpace(line)
				if v != "" && strings.HasPrefix(v, `"`) || (v != "" && regexp.MustCompile(`^[\w.]+\s+"`).MatchString(v)) {
					if !imports[v] {
						imports[v] = true
					}
				}
			}
		}
		code = regexp.MustCompile(`(?m)^\s*package\s+\w+\s*\n?`).ReplaceAllString(code, "")
		code = regexp.MustCompile(`(?m)^\s*import\s+((?:[\w.]+\s+)?"[^"\n]+")\s*\n?`).ReplaceAllString(code, "")
		code = regexp.MustCompile(`(?s)import\s*\(.*?\)\s*`).ReplaceAllString(code, "")
		cfg.Handlers[i].Code = strings.TrimSpace(code)
	}
	for v := range imports {
		importLines = append(importLines, v)
	}
	sort.Strings(importLines)
	var b strings.Builder
	b.WriteString("package main\n")
	if len(importLines) == 1 {
		b.WriteString("\nimport " + importLines[0] + "\n")
	} else if len(importLines) > 1 {
		b.WriteString("\nimport (\n")
		for _, v := range importLines {
			b.WriteString("\t" + v + "\n")
		}
		b.WriteString(")\n")
	}
	// Preserve the user's existing shared declarations after normalizing imports.
	if global != "" {
		body := regexp.MustCompile(`(?m)^\s*package\s+\w+\s*\n?`).ReplaceAllString(global, "")
		body = regexp.MustCompile(`(?m)^\s*import\s+((?:[\w.]+\s+)?"[^"\n]+")\s*\n?`).ReplaceAllString(body, "")
		body = regexp.MustCompile(`(?s)import\s*\(.*?\)\s*`).ReplaceAllString(body, "")
		if strings.TrimSpace(body) != "" {
			b.WriteString("\n" + strings.TrimSpace(body) + "\n")
		}
	}
	cfg.GlobalScript = strings.TrimSpace(b.String())
}

func DeleteServerConfig(id string) error {
	if err := validID(id); err != nil {
		return err
	}

	storeMu.Lock()
	defer storeMu.Unlock()

	filePath := filepath.Join(serverDir, id+".json")
	_ = os.Remove(filePath)
	logPath := filepath.Join(logsDir, id+".log")
	_ = os.Remove(logPath)
	_ = os.RemoveAll(filepath.Join(depsDir, id))

	logMu.Lock()
	delete(logsCache, id)
	logMu.Unlock()

	return nil
}

func AppendLog(serverID string, message string) {
	if validID(serverID) != nil {
		return
	}

	logMu.Lock()
	defer logMu.Unlock()

	EnsureDirs()

	timestamp := time.Now().Format("15:04:05")
	formattedMsg := fmt.Sprintf("[%s] %s", timestamp, message)

	logsCache[serverID] = append(logsCache[serverID], formattedMsg)

	// Persist log file
	logFilePath := filepath.Join(logsDir, serverID+".log")
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		_, _ = f.WriteString(formattedMsg + "\n")
		f.Close()
	}
}

func GetLogs(serverID string) []string {
	if validID(serverID) != nil {
		return []string{}
	}

	logMu.RLock()
	defer logMu.RUnlock()

	if cached, ok := logsCache[serverID]; ok && len(cached) > 0 {
		return cached
	}

	logFilePath := filepath.Join(logsDir, serverID+".log")
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		return []string{}
	}

	var lines []string
	current := ""
	for _, b := range data {
		if b == '\n' {
			if current != "" {
				lines = append(lines, current)
				current = ""
			}
		} else {
			current += string(b)
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	logsCache[serverID] = lines
	return lines
}
