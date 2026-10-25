package managers

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"server-manager/Data"
	"server-manager/Runtime"
	httpengine "server-manager/Servers/HTTP"
	tcpengine "server-manager/Servers/TCP"
	udpengine "server-manager/Servers/UDP"
	wsengine "server-manager/Servers/WebSocket"
)

type ServerInstance interface {
	Start() error
	Stop() error
	IsRunning() bool
}

type Manager struct {
	mu        sync.RWMutex
	instances map[string]ServerInstance

	installMu  sync.Mutex
	installing map[string]bool
}

var GlobalManager = NewManager()

func NewManager() *Manager {
	return &Manager{
		instances:  make(map[string]ServerInstance),
		installing: make(map[string]bool),
	}
}

func (m *Manager) StartServer(id string) (startErr error) {
	// Make sure third-party handler packages are installed before Yaegi compiles code.
	// This also handles older servers and clicks made while creation-time install runs.
	if id != "" {
		if cfg, err := data.LoadServerConfig(id); err == nil && cfg != nil {
			// Repair configs created by earlier builds and infer required packages from
			// their starter handlers, so old servers receive the same defaults as new ones.
			changed := false
			// Older builds incorrectly added Gorilla to every protocol. Keep it only
			// for WebSocket servers; it is not a default dependency for HTTP/TCP/UDP.
			if cfg.Proto != "ws" {
				filtered := cfg.Dependencies[:0]
				for _, dep := range cfg.Dependencies {
					if dep.Path != "github.com/gorilla/websocket" {
						filtered = append(filtered, dep)
					} else {
						changed = true
					}
				}
				cfg.Dependencies = filtered
			}
			// Dependencies now live in GlobalScript, so inspect that script as well
			// as legacy individual handlers. Otherwise older WS servers would lose
			// Gorilla during migration and fail when the global script is evaluated.
			if cfg.Proto == "ws" && strings.Contains(cfg.GlobalScript, "github.com/gorilla/websocket") {
				found := false
				for _, dep := range cfg.Dependencies {
					if dep.Path == "github.com/gorilla/websocket" {
						found = true
						break
					}
				}
				if !found {
					cfg.Dependencies = append(cfg.Dependencies, data.Dependency{Path: "github.com/gorilla/websocket", Version: "v1.5.3"})
					changed = true
				}
			}
			for i := range cfg.Handlers {
				if (cfg.Proto == "ws" || cfg.Proto == "tcp" || cfg.Proto == "udp") && strings.TrimSpace(cfg.Handlers[i].Path) == "" {
					cfg.Handlers[i].Path = "*"
					changed = true
				}
				if strings.Contains(cfg.Handlers[i].Code, `import "http/net"`) {
					cfg.Handlers[i].Code = strings.ReplaceAll(cfg.Handlers[i].Code, `import "http/net"`, `import "net/http"`)
					changed = true
				}
				if cfg.Proto == "ws" && strings.Contains(cfg.Handlers[i].Code, `github.com/gorilla/websocket`) {
					found := false
					for _, dep := range cfg.Dependencies {
						if dep.Path == "github.com/gorilla/websocket" {
							found = true
							break
						}
					}
					if !found {
						cfg.Dependencies = append(cfg.Dependencies, data.Dependency{Path: "github.com/gorilla/websocket", Version: "v1.5.3"})
						changed = true
					}
				}
			}
			if changed {
				_ = data.SaveServerConfig(cfg)
			}
			needsInstall := false
			for _, dep := range cfg.Dependencies {
				if dep.Status != "installed" {
					needsInstall = true
					break
				}
			}
			if needsInstall {
				deadline := time.Now().Add(5 * time.Minute)
				for {
					res := m.InstallDependencies(id)
					if res.OK {
						break
					}
					if !strings.Contains(res.Message, "ya hay una instalación en curso") {
						return fmt.Errorf("no se pudieron preparar las dependencias del servidor: %s", res.Message)
					}
					if time.Now().After(deadline) {
						return fmt.Errorf("tiempo agotado esperando la instalación de dependencias")
					}
					time.Sleep(200 * time.Millisecond)
					cfg, err = data.LoadServerConfig(id)
					if err != nil {
						return fmt.Errorf("no se pudo releer la configuración: %w", err)
					}
					allInstalled := true
					for _, dep := range cfg.Dependencies {
						if dep.Status != "installed" {
							allInstalled = false
							break
						}
					}
					if allInstalled {
						break
					}
				}
			}
		}
	}
	// A malformed legacy config or a bug in a protocol engine must not crash
	// the UI WebSocket handler or the manager process. Keep the stack in logs
	// so the actual source of a panic can be diagnosed instead of guessing.
	defer func() {
		if recovered := recover(); recovered != nil {
			// Include the stack in the error returned to the UI, not only stdout.
			// Otherwise the UI displays just the generic nil-pointer message and
			// the source line needed to diagnose it is hidden from the user.
			stack := debug.Stack()
			startErr = fmt.Errorf("panic al iniciar el servidor %s: %v\nTraza para diagnóstico:\n%s", id, recovered, stack)
			log.Printf("StartServer panic (%s): %v\n%s", id, recovered, stack)
			// Cleanup is best-effort: cleanup errors must not mask the original panic.
			func() {
				defer func() { _ = recover() }()
				scriptrt.Release(id)
				data.AppendLog(id, startErr.Error())
				setStatus(id, "error")
			}()
		}
	}()
	if m == nil {
		return fmt.Errorf("el administrador de servidores no está inicializado")
	}
	if id == "" {
		return fmt.Errorf("falta el ID del servidor")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	cfg, err := data.LoadServerConfig(id)
	if err != nil {
		return fmt.Errorf("failed to load server config: %w", err)
	}
	if cfg == nil {
		return fmt.Errorf("la configuración del servidor %s está vacía", id)
	}
	if cfg.ID == "" {
		cfg.ID = id
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("puerto inválido para el servidor: %d", cfg.Port)
	}
	// Older saved configs predate the default environment/database/dependency
	// fields. Normalize them in memory so nil slices never surprise engines.
	if cfg.Handlers == nil {
		cfg.Handlers = []data.HandlerConfig{}
	}
	if cfg.Env == nil {
		cfg.Env = []data.EnvVar{}
	}
	if cfg.Databases == nil {
		cfg.Databases = []data.DBConfig{}
	}
	if cfg.Dependencies == nil {
		cfg.Dependencies = []data.Dependency{}
	}

	if inst, ok := m.instances[id]; ok && inst.IsRunning() {
		return fmt.Errorf("server %s is already running", id)
	}

	var inst ServerInstance
	switch cfg.Proto {
	case "http":
		inst = httpengine.NewHTTPServer(cfg)
	case "ws":
		inst = wsengine.NewWSServer(cfg)
	case "tcp":
		inst = tcpengine.NewTCPServer(cfg)
	case "udp":
		inst = udpengine.NewUDPServer(cfg)
	default:
		return fmt.Errorf("unsupported protocol: %s", cfg.Proto)
	}

	// Fresh runtime for this run: env vars, DB pools and dependencies come from cfg.
	scriptrt.Acquire(cfg)

	if err := inst.Start(); err != nil {
		scriptrt.Release(id)
		setStatus(id, "error")
		return err
	}

	m.instances[id] = inst
	setStatus(id, "running")

	return nil
}

// setStatus updates only the status field, atomically, so it can never
// overwrite settings saved concurrently from the UI.
func setStatus(id, status string) {
	_, _ = data.UpdateServerConfig(id, func(cfg *data.ServerConfig, _ []*data.ServerConfig) error {
		cfg.Status = status
		return nil
	})
}

// AutoStartAll runs once when the application boots. Nothing survives a
// restart of the manager, so any stored "running" status is stale and gets
// reset; servers flagged as autostart are then started.
func (m *Manager) AutoStartAll() {
	list, err := data.ListServerConfigs()
	if err != nil {
		return
	}

	for _, cfg := range list {
		if cfg.Status != "stopped" {
			setStatus(cfg.ID, "stopped")
		}
		if !cfg.AutoStart {
			continue
		}
		if err := m.StartServer(cfg.ID); err != nil {
			log.Printf("autostart %s: %v", cfg.ID, err)
			data.AppendLog(cfg.ID, fmt.Sprintf("Autostart failed: %v", err))
		}
	}
}

func (m *Manager) StopServer(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := data.LoadServerConfig(id); err != nil {
		return fmt.Errorf("failed to load server config: %w", err)
	}

	if inst, ok := m.instances[id]; ok {
		_ = inst.Stop()
		delete(m.instances, id)
	}
	scriptrt.Release(id)

	setStatus(id, "stopped")

	return nil
}

func (m *Manager) GetServerStatus(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if inst, ok := m.instances[id]; ok && inst.IsRunning() {
		return "running"
	}

	cfg, err := data.LoadServerConfig(id)
	if err == nil {
		return cfg.Status
	}

	return "stopped"
}
