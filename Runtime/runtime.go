// Package scriptrt builds the Yaegi interpreters that run the handlers of a
// server. Every running server owns one Runtime that carries what the handlers
// can see: their environment variables, the downloaded third-party
// dependencies, and the "server-manager/sdk" package (database connections and
// logging).
package scriptrt

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"server-manager/Data"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

// SDKImportPath is the import path handlers use to reach the SDK:
//
//	import "server-manager/sdk"
const SDKImportPath = "server-manager/sdk"

// Runtime is the per-server execution context shared by all its handlers.
type Runtime struct {
	cfg *data.ServerConfig

	mu     sync.Mutex
	dbs    map[string]*sql.DB
	closed bool
}

var (
	regMu    sync.Mutex
	registry = map[string]*Runtime{}
)

// Acquire creates a fresh Runtime for the server, closing the previous one if
// there was one. The manager calls it every time a server starts so that
// changes to env vars, databases or dependencies are picked up.
func Acquire(cfg *data.ServerConfig) *Runtime {
	regMu.Lock()
	defer regMu.Unlock()

	if old := registry[cfg.ID]; old != nil {
		old.Close()
	}
	rt := newRuntime(cfg)
	registry[cfg.ID] = rt
	return rt
}

// Release closes the server's Runtime (and its database pools).
func Release(id string) {
	regMu.Lock()
	rt := registry[id]
	delete(registry, id)
	regMu.Unlock()

	if rt != nil {
		rt.Close()
	}
}

// NewInterpreter returns a ready-to-use interpreter for one handler of the
// server described by cfg.
func NewInterpreter(cfg *data.ServerConfig) *interp.Interpreter {
	regMu.Lock()
	rt := registry[cfg.ID]
	if rt == nil {
		rt = newRuntime(cfg)
		registry[cfg.ID] = rt
	}
	regMu.Unlock()

	return rt.NewInterpreter()
}

func newRuntime(cfg *data.ServerConfig) *Runtime {
	return &Runtime{cfg: cfg, dbs: map[string]*sql.DB{}}
}

// NewInterpreter creates an interpreter with the stdlib, the server env vars,
// the downloaded dependencies and the SDK.
func (rt *Runtime) NewInterpreter() *interp.Interpreter {
	opts := interp.Options{}

	// os.Getenv inside a handler reads this virtual environment, not the real one.
	for _, e := range rt.cfg.Env {
		opts.Env = append(opts.Env, e.Key+"="+e.Value)
	}

	// Third-party packages are interpreted from source out of <deps>/src.
	if len(rt.cfg.Dependencies) > 0 {
		if dir, err := data.DepsDir(rt.cfg.ID); err == nil {
			opts.GoPath = dir
		}
	}

	i := interp.New(opts)
	_ = i.Use(stdlib.Symbols)
	_ = i.Use(rt.sdkExports())
	return i
}

func (rt *Runtime) sdkExports() interp.Exports {
	id := rt.cfg.ID
	return interp.Exports{
		// Key format is "<import path>/<package name>"
		SDKImportPath + "/sdk": {
			"DB": reflect.ValueOf(rt.DB),
			"Log": reflect.ValueOf(func(msg string) {
				data.AppendLog(id, "[script] "+msg)
			}),
			"Logf": reflect.ValueOf(func(format string, args ...interface{}) {
				data.AppendLog(id, "[script] "+fmt.Sprintf(format, args...))
			}),
		},
	}
}

// DB returns the pool for a database connection configured on the server.
// Pools are opened lazily and live until the server stops.
func (rt *Runtime) DB(name string) (*sql.DB, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.closed {
		return nil, fmt.Errorf("sdk.DB(%q): the server is stopping", name)
	}
	if db := rt.dbs[name]; db != nil {
		return db, nil
	}

	var names []string
	for _, c := range rt.cfg.Databases {
		names = append(names, c.Name)
		if c.Name != name {
			continue
		}

		db, err := sql.Open(c.Driver, c.DSN)
		if err != nil {
			return nil, fmt.Errorf("sdk.DB(%q): %w", name, err)
		}
		db.SetMaxOpenConns(10)
		db.SetConnMaxLifetime(30 * time.Minute)
		rt.dbs[name] = db
		return db, nil
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("sdk.DB(%q): this server has no database connections configured", name)
	}
	return nil, fmt.Errorf("sdk.DB(%q): no such connection (configured: %s)", name, strings.Join(names, ", "))
}

// Close releases every database pool.
func (rt *Runtime) Close() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.closed = true
	for name, db := range rt.dbs {
		_ = db.Close()
		delete(rt.dbs, name)
	}
}

// Drivers lists the database/sql drivers compiled into the binary.
func Drivers() []string {
	names := sql.Drivers()
	sort.Strings(names)
	return names
}

// TestDB opens a connection and pings it.
func TestDB(ctx context.Context, driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// EvalServerHandler evaluates the server-wide script and the selected handler in
// one interpreter. Imports are evaluated separately from declarations because
// Yaegi can report "missing import path" when package/import blocks are repeatedly
// concatenated with handler code. The UI still stores global and handler scripts
// separately; this function only assembles them at runtime.
func EvalServerHandler(i *interp.Interpreter, cfg *data.ServerConfig, code, name string) (reflect.Value, error) {
	if i == nil || cfg == nil {
		return reflect.Value{}, fmt.Errorf("runtime/interpreter no inicializado")
	}
	if strings.TrimSpace(code) == "" {
		return reflect.Value{}, fmt.Errorf("script del handler vacío")
	}

	global := strings.TrimSpace(cfg.GlobalScript)
	if global == "" {
		global = "package main"
	}

	imports, globalBody, err := splitScript(global)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("script global: %w", err)
	}
	handlerImports, handlerBody, err := splitScript(code)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("handler %s: %w", name, err)
	}

	// Register imports in one valid Go import declaration. Evaluating fragments
	// one-by-one can leave Yaegi parsing a malformed import in older configs.
	seen := make(map[string]bool)
	uniqueImports := make([]string, 0, len(imports)+len(handlerImports))
	for _, spec := range append(imports, handlerImports...) {
		spec = strings.TrimSpace(spec)
		if spec == "" || seen[spec] {
			continue
		}
		seen[spec] = true
		uniqueImports = append(uniqueImports, spec)
	}
	if len(uniqueImports) > 0 {
		var importBlock strings.Builder
		importBlock.WriteString("import (\n")
		for _, spec := range uniqueImports {
			importBlock.WriteString("\t" + spec + "\n")
		}
		importBlock.WriteString(")")
		if _, err := i.Eval(importBlock.String()); err != nil {
			return reflect.Value{}, fmt.Errorf("imports globales para handler %s (%s): %w", name, strings.Join(uniqueImports, ", "), err)
		}
	}

	if strings.TrimSpace(globalBody) != "" {
		if _, err := i.Eval(globalBody); err != nil {
			return reflect.Value{}, fmt.Errorf("declaraciones del script global para %s: %w", name, err)
		}
	}
	if strings.TrimSpace(handlerBody) == "" {
		return reflect.Value{}, fmt.Errorf("el handler %s no contiene una función", name)
	}
	if _, err := i.Eval(handlerBody); err != nil {
		return reflect.Value{}, fmt.Errorf("código del handler %s: %w", name, err)
	}
	v, err := i.Eval(name)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("resolver handler %s: %w", name, err)
	}
	if !v.IsValid() {
		return reflect.Value{}, fmt.Errorf("handler %s no válido", name)
	}
	return v, nil
}

// splitScript extracts import specs and returns declarations without package or
// import clauses. It supports both `import "pkg"` and grouped import blocks.
func splitScript(source string) ([]string, string, error) {
	packageRE := regexp.MustCompile(`(?m)^\s*package\s+[A-Za-z_][A-Za-z0-9_]*\s*\n?`)
	singleImportRE := regexp.MustCompile(`(?m)^\s*import\s+((?:[A-Za-z_][A-Za-z0-9_]*\s+)?"[^"\n]+")\s*\n?`)
	groupImportRE := regexp.MustCompile(`(?s)import\s*\((.*?)\)`)
	specRE := regexp.MustCompile(`^(?:(\w+)\s+)?("[^"\n]+")$`)

	var imports []string
	clean := source
	for _, match := range groupImportRE.FindAllStringSubmatch(clean, -1) {
		for _, line := range strings.Split(match[1], "\n") {
			line = strings.TrimSpace(strings.SplitN(line, "//", 2)[0])
			line = strings.ReplaceAll(line, `"http/net"`, `"net/http"`)
			if line == "" {
				continue
			}
			if !specRE.MatchString(line) {
				return nil, "", fmt.Errorf("declaración de import inválida: %q", line)
			}
			imports = append(imports, line)
		}
	}
	clean = groupImportRE.ReplaceAllString(clean, "\n")
	for _, match := range singleImportRE.FindAllStringSubmatch(clean, -1) {
		spec := strings.TrimSpace(match[1])
		if strings.Contains(spec, `"http/net"`) {
			spec = strings.ReplaceAll(spec, `"http/net"`, `"net/http"`)
		}
		imports = append(imports, spec)
	}
	clean = singleImportRE.ReplaceAllString(clean, "\n")
	clean = packageRE.ReplaceAllString(clean, "\n")
	return imports, strings.TrimSpace(clean), nil
}

// EvalHandler remains available for code that does not require a server global
// script. Server protocol engines should use EvalServerHandler instead.
func EvalHandler(i *interp.Interpreter, code, name string) (reflect.Value, error) {
	if i == nil {
		return reflect.Value{}, fmt.Errorf("interpreter no inicializado")
	}
	if strings.TrimSpace(code) == "" {
		return reflect.Value{}, fmt.Errorf("script del handler vacío")
	}
	if _, err := i.Eval(code); err != nil {
		return reflect.Value{}, fmt.Errorf("handler %s: %w", name, err)
	}
	v, err := i.Eval(name)
	if err != nil || !v.IsValid() {
		if err == nil {
			err = fmt.Errorf("handler no válido")
		}
		return reflect.Value{}, fmt.Errorf("resolver handler %s: %w", name, err)
	}
	return v, nil
}
