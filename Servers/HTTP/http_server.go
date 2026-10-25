package httpengine

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"server-manager/Data"
	"server-manager/Runtime"

	"github.com/labstack/echo/v4"
)

type HTTPServer struct {
	cfg     *data.ServerConfig
	echo    *echo.Echo
	srv     *http.Server
	mu      sync.Mutex
	running bool
}

func NewHTTPServer(cfg *data.ServerConfig) *HTTPServer {
	return &HTTPServer{
		cfg: cfg,
	}
}

func (s *HTTPServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("HTTP server already running")
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Custom logger middleware logging to server logs
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			latency := time.Since(start)
			req := c.Request()
			res := c.Response()
			msg := fmt.Sprintf("%s %s %d %v", req.Method, req.URL.Path, res.Status, latency)
			data.AppendLog(s.cfg.ID, msg)
			return err
		}
	})

	// Register default root handler if none present
	if len(s.cfg.Handlers) == 0 {
		e.GET("/", func(c echo.Context) error {
			return c.String(http.StatusOK, fmt.Sprintf("HTTP Server '%s' running on port %d", s.cfg.Name, s.cfg.Port))
		})
	}

	// Dynamic handlers using Yaegi
	for _, h := range s.cfg.Handlers {
		handlerCopy := h
		if strings.TrimSpace(handlerCopy.Code) == "" {
			continue
		}

		path := handlerCopy.Path
		if path == "" {
			path = "/"
		}
		method := strings.ToUpper(handlerCopy.Method)
		if method == "" {
			method = "GET"
		}

		// Compile code in Yaegi
		interpEngine := scriptrt.NewInterpreter(s.cfg)

		funcVal, err := scriptrt.EvalServerHandler(interpEngine, s.cfg, handlerCopy.Code, handlerCopy.Name)

		if err != nil || !funcVal.IsValid() {
			if err == nil {
				err = fmt.Errorf("la función del handler no es válida")
			}
			msg := fmt.Sprintf("Error resolving handler function %s: %v", handlerCopy.Name, err)
			data.AppendLog(s.cfg.ID, msg)
			s.mu.Unlock()
			return fmt.Errorf("%s", msg)
		}

		// Support func(c echo.Context) error OR func(w http.ResponseWriter, r *http.Request)
		handlerFunc := funcVal.Interface()

		switch fn := handlerFunc.(type) {
		case func(echo.Context) error:
			e.Add(method, path, fn)
		case func(http.ResponseWriter, *http.Request):
			e.Add(method, path, func(c echo.Context) error {
				fn(c.Response().Writer, c.Request())
				return nil
			})
		default:
			msg := fmt.Sprintf("Handler %s has unsupported signature: %T", handlerCopy.Name, handlerFunc)
			data.AppendLog(s.cfg.ID, msg)
			s.mu.Unlock()
			return fmt.Errorf("%s", msg)
		}
	}

	addr := fmt.Sprintf(":%d", s.cfg.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: e,
	}

	s.echo = e
	s.srv = srv
	s.running = true
	s.mu.Unlock()

	data.AppendLog(s.cfg.ID, fmt.Sprintf("HTTP server started on port %d", s.cfg.Port))

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			data.AppendLog(s.cfg.ID, fmt.Sprintf("HTTP server error: %v", err))
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}
	}()

	return nil
}

func (s *HTTPServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running || s.srv == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := s.srv.Shutdown(ctx)
	s.running = false
	data.AppendLog(s.cfg.ID, "HTTP server stopped")
	return err
}

func (s *HTTPServer) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func extractPackageName(code string) string {
	lines := strings.Split(code, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "package ") {
			return strings.TrimSpace(strings.TrimPrefix(l, "package "))
		}
	}
	return "main"
}
