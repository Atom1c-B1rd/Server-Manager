package managers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"server-manager/Data"
	"server-manager/Runtime"
)

var (
	envKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	dbNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
)

// SettingsInput is what the UI sends when the user saves a server's settings.
type SettingsInput struct {
	Name         string            `json:"name"`
	Port         int               `json:"port"`
	AutoStart    bool              `json:"autostart"`
	Env          []data.EnvVar     `json:"env"`
	Databases    []data.DBConfig   `json:"databases"`
	Dependencies []data.Dependency `json:"dependencies"`
}

type SettingsResult struct {
	Restarted    bool   `json:"restarted"`
	RestartError string `json:"restart_error,omitempty"`
}

// normalize trims and validates the input. Completely empty rows are dropped.
func (in *SettingsInput) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return fmt.Errorf("el nombre no puede estar vacío")
	}
	if utf8.RuneCountInString(in.Name) > 64 {
		return fmt.Errorf("el nombre es demasiado largo (máximo 64 caracteres)")
	}
	if in.Port < 1 || in.Port > 65535 {
		return fmt.Errorf("el puerto debe estar entre 1 y 65535")
	}

	var env []data.EnvVar
	seenEnv := map[string]bool{}
	for _, e := range in.Env {
		e.Key = strings.TrimSpace(e.Key)
		if e.Key == "" && e.Value == "" {
			continue
		}
		if !envKeyRe.MatchString(e.Key) {
			return fmt.Errorf("variable de entorno %q: el nombre solo puede tener letras, números y _ (y no empezar con un número)", e.Key)
		}
		if seenEnv[e.Key] {
			return fmt.Errorf("variable de entorno %q repetida", e.Key)
		}
		seenEnv[e.Key] = true
		env = append(env, e)
	}
	in.Env = env

	drivers := map[string]bool{}
	for _, d := range scriptrt.Drivers() {
		drivers[d] = true
	}
	var dbs []data.DBConfig
	seenDB := map[string]bool{}
	for _, d := range in.Databases {
		d.Name, d.Driver, d.DSN = strings.TrimSpace(d.Name), strings.TrimSpace(d.Driver), strings.TrimSpace(d.DSN)
		if d.Name == "" && d.Driver == "" && d.DSN == "" {
			continue
		}
		if !dbNameRe.MatchString(d.Name) {
			return fmt.Errorf("conexión %q: el nombre debe tener entre 1 y 32 caracteres (letras, números, - y _)", d.Name)
		}
		if seenDB[d.Name] {
			return fmt.Errorf("conexión %q repetida", d.Name)
		}
		seenDB[d.Name] = true
		if !drivers[d.Driver] {
			return fmt.Errorf("conexión %q: el driver %q no está disponible (disponibles: %s)", d.Name, d.Driver, strings.Join(scriptrt.Drivers(), ", "))
		}
		if d.DSN == "" {
			return fmt.Errorf("conexión %q: falta la cadena de conexión (DSN)", d.Name)
		}
		dbs = append(dbs, d)
	}
	in.Databases = dbs

	var deps []data.Dependency
	seenDep := map[string]bool{}
	for _, d := range in.Dependencies {
		if strings.TrimSpace(d.Path) == "" && strings.TrimSpace(d.Version) == "" {
			continue
		}
		d, err := scriptrt.NormalizeDependency(d)
		if err != nil {
			return err
		}
		if seenDep[d.Path] {
			return fmt.Errorf("dependencia %q repetida", d.Path)
		}
		seenDep[d.Path] = true
		deps = append(deps, data.Dependency{Path: d.Path, Version: d.Version})
	}
	in.Dependencies = deps

	return nil
}

// UpdateSettings validates and stores a server's settings. If the server is
// running and a change affects its runtime (port, env vars, databases) it is
// restarted so the change takes effect.
func (m *Manager) UpdateSettings(id string, in SettingsInput) (*SettingsResult, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}

	runtimeChanged := false
	_, err := data.UpdateServerConfig(id, func(cfg *data.ServerConfig, others []*data.ServerConfig) error {
		if in.Port != cfg.Port {
			for _, o := range others {
				if o.Port == in.Port && portFamily(o.Proto) == portFamily(cfg.Proto) {
					return fmt.Errorf("el puerto %d ya lo usa el server «%s»", in.Port, o.Name)
				}
			}
		}

		runtimeChanged = in.Port != cfg.Port ||
			!reflect.DeepEqual(nilIfEmpty(cfg.Env), nilIfEmpty(in.Env)) ||
			!reflect.DeepEqual(nilIfEmpty(cfg.Databases), nilIfEmpty(in.Databases))

		cfg.Name = in.Name
		cfg.Port = in.Port
		cfg.AutoStart = in.AutoStart
		cfg.Env = in.Env
		cfg.Databases = in.Databases
		cfg.Dependencies = mergeDeps(cfg.Dependencies, in.Dependencies)
		return nil
	})
	if err != nil {
		return nil, err
	}

	res := &SettingsResult{}
	if runtimeChanged && m.isRunning(id) {
		res.Restarted = true
		if err := m.restart(id); err != nil {
			res.RestartError = err.Error()
		}
	}
	return res, nil
}

// InstallResult is the outcome of InstallDependencies.
type InstallResult struct {
	OK        bool   `json:"ok"`
	Message   string `json:"message"`
	Restarted bool   `json:"restarted"`
}

// InstallDependencies downloads the saved dependencies of a server and, if it
// is running, restarts it so the handlers see them. It can take a while
// (network), so callers should run it in a goroutine.
func (m *Manager) InstallDependencies(id string) InstallResult {
	m.installMu.Lock()
	if m.installing[id] {
		m.installMu.Unlock()
		return InstallResult{Message: "ya hay una instalación en curso para este server"}
	}
	m.installing[id] = true
	m.installMu.Unlock()
	defer func() {
		m.installMu.Lock()
		delete(m.installing, id)
		m.installMu.Unlock()
	}()

	cfg, err := data.LoadServerConfig(id)
	if err != nil {
		return InstallResult{Message: err.Error()}
	}
	if len(cfg.Dependencies) == 0 {
		return InstallResult{Message: "este server no tiene dependencias configuradas"}
	}
	installed := append([]data.Dependency(nil), cfg.Dependencies...)

	data.AppendLog(id, fmt.Sprintf("[deps] Instalando %d dependencia(s)...", len(installed)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	installErr := scriptrt.InstallDeps(ctx, id, installed, func(line string) {
		data.AppendLog(id, "[deps] "+line)
	})

	// Record the outcome only on the entries that were actually installed
	// (the user may have edited the list while the download was running).
	_, _ = data.UpdateServerConfig(id, func(c *data.ServerConfig, _ []*data.ServerConfig) error {
		for i := range c.Dependencies {
			for _, d := range installed {
				if c.Dependencies[i].Path != d.Path || c.Dependencies[i].Version != d.Version {
					continue
				}
				if installErr != nil {
					c.Dependencies[i].Status, c.Dependencies[i].Message = "error", installErr.Error()
				} else {
					c.Dependencies[i].Status, c.Dependencies[i].Message = "installed", ""
				}
			}
		}
		return nil
	})

	if installErr != nil {
		data.AppendLog(id, "[deps] Error: "+installErr.Error())
		return InstallResult{Message: installErr.Error()}
	}
	data.AppendLog(id, "[deps] Dependencias instaladas")

	res := InstallResult{OK: true, Message: "dependencias instaladas"}
	if m.isRunning(id) {
		res.Restarted = true
		if err := m.restart(id); err != nil {
			res.Message = "dependencias instaladas, pero no se pudo reiniciar el server: " + err.Error()
		}
	}
	return res
}

func (m *Manager) isRunning(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inst, ok := m.instances[id]
	return ok && inst.IsRunning()
}

func (m *Manager) restart(id string) error {
	if err := m.StopServer(id); err != nil {
		return err
	}
	return m.StartServer(id)
}

// portFamily groups protocols that compete for the same port space.
func portFamily(proto string) string {
	if proto == "udp" {
		return "udp"
	}
	return "tcp"
}

func nilIfEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}

// mergeDeps keeps the install status of dependencies that did not change and
// leaves new or modified ones pending.
func mergeDeps(old, incoming []data.Dependency) []data.Dependency {
	var out []data.Dependency
	for _, d := range incoming {
		for _, o := range old {
			if o.Path == d.Path && o.Version == d.Version {
				d.Status, d.Message = o.Status, o.Message
				break
			}
		}
		out = append(out, d)
	}
	return out
}
