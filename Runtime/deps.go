package scriptrt

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"server-manager/Data"
)

var (
	// A package import path: the first element must look like a domain
	// (github.com/..., gopkg.in/..., rsc.io/...), which also rules out stdlib
	// paths and anything starting with "-".
	depPathRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~-]*\.[A-Za-z0-9._~-]+(/[A-Za-z0-9._~-]+)+$`)

	// "latest" or a semantic / pseudo version such as v1.6.0 or v0.0.0-2024...-abcdef
	depVersionRe = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.+-]+)?$`)
)

// NormalizeDependency validates a dependency and fills in the default version.
func NormalizeDependency(d data.Dependency) (data.Dependency, error) {
	d.Path = strings.TrimSpace(d.Path)
	d.Version = strings.TrimSpace(d.Version)
	if d.Version == "" {
		d.Version = "latest"
	}

	if !depPathRe.MatchString(d.Path) || strings.Contains(d.Path, "..") {
		return d, fmt.Errorf("dependencia %q: ruta de paquete inválida (ejemplo: github.com/google/uuid)", d.Path)
	}
	if d.Version != "latest" && !depVersionRe.MatchString(d.Version) {
		return d, fmt.Errorf("dependencia %q: versión %q inválida (usá \"latest\" o algo como v1.6.0)", d.Path, d.Version)
	}
	return d, nil
}

// InstallDeps downloads the dependencies (and everything they import) into the
// server's private GOPATH so the interpreter can load them from source.
//
// It needs the `go` command and network access. The work happens in a scratch
// module: `go get` + `go mod tidy` resolve versions, `go mod vendor` collects
// the sources, and the vendor tree becomes <deps>/src.
func InstallDeps(ctx context.Context, id string, deps []data.Dependency, logf func(string)) error {
	if len(deps) == 0 {
		return fmt.Errorf("no hay dependencias para instalar")
	}

	goBin, err := exec.LookPath("go")
	if err != nil {
		return fmt.Errorf("no se encontró el comando 'go' en el PATH: instalá Go para poder descargar dependencias")
	}

	root, err := data.DepsDir(id)
	if err != nil {
		return err
	}
	work := filepath.Join(root, ".build")
	_ = os.RemoveAll(work)
	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(work)

	var imports strings.Builder
	gets := []string{"get"}
	for _, d := range deps {
		d, err := NormalizeDependency(d)
		if err != nil {
			return err
		}
		fmt.Fprintf(&imports, "\t_ %q\n", d.Path)
		gets = append(gets, d.Path+"@"+d.Version)
	}

	if err := os.WriteFile(filepath.Join(work, "go.mod"), []byte("module smdeps\n\ngo 1.21\n"), 0o644); err != nil {
		return err
	}
	stub := "package smdeps\n\nimport (\n" + imports.String() + ")\n"
	if err := os.WriteFile(filepath.Join(work, "deps.go"), []byte(stub), 0o644); err != nil {
		return err
	}

	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, goBin, args...)
		cmd.Dir = work
		// Ignore the user's GOFLAGS/GOWORK: this scratch module must resolve on its own.
		cmd.Env = append(os.Environ(), "GOFLAGS=", "GOWORK=off", "GO111MODULE=on")
		out, err := cmd.CombinedOutput()
		text := strings.TrimSpace(string(out))
		if text != "" && logf != nil {
			logf("go " + strings.Join(args[:1], " ") + ": " + text)
		}
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("go %s: %w", args[0], ctx.Err())
			}
			return fmt.Errorf("go %s: %s", args[0], lastLines(text, 6))
		}
		return nil
	}

	for _, args := range [][]string{gets, {"mod", "tidy"}, {"mod", "vendor"}} {
		if err := run(args...); err != nil {
			return err
		}
	}

	src := filepath.Join(root, "src")
	if err := os.RemoveAll(src); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(work, "vendor"), src); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(src, "modules.txt"))
	return nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
