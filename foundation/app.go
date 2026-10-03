// Package foundation provides the core application lifecycle manager for microservices.
// It features standardized startup sequences, centralized graceful shutdown coordination,
// and integrated Kubernetes-ready health checks.
package foundation

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhbvha/go-common/health"
	"github.com/thanhbvha/go-common/utils/graceful"
)

// Config represents the core application configuration.
type Config struct {
	Name    string
	Version string

	// ShutdownTimeout determines how long to wait for graceful shutdown
	ShutdownTimeout time.Duration
}

// App represents the lifecycle manager for a microservice.
type App struct {
	cfg      Config
	shutdown *graceful.Shutdown
	health   *health.Checker

	// startFuncs stores background routines or servers to start.
	startFuncs []func(context.Context) error
}

// NewApp creates a new application lifecycle manager.
func NewApp(cfg Config) *App {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	app := &App{
		cfg:      cfg,
		shutdown: graceful.NewShutdown(cfg.ShutdownTimeout),
		health:   health.New().WithVersion(cfg.Version),
	}

	return app
}

// OnStart registers a function that will be executed when App.Run is called.
// Usually used to start HTTP/gRPC servers or workers in the background.
// The function must not block indefinitely; it should start its work in a goroutine
// if necessary, or return immediately after initialization.
func (a *App) OnStart(fn func(ctx context.Context) error) {
	a.startFuncs = append(a.startFuncs, fn)
}

// OnShutdown registers a cleanup function to run during graceful shutdown.
func (a *App) OnShutdown(fn func(ctx context.Context) error) {
	a.shutdown.Register(fn)
}

// Health returns the global health checker for this app.
func (a *App) Health() *health.Checker {
	return a.health
}

// Run starts all registered components and blocks until an OS signal is received.
// When a signal is received, it triggers a graceful shutdown.
func (a *App) Run() error {
	ctx := context.Background()

	// Execute all start functions
	log.Printf("[%s] Starting application version %s...", a.cfg.Name, a.cfg.Version)
	for i, startFn := range a.startFuncs {
		if err := startFn(ctx); err != nil {
			log.Printf("[%s] Failed to start component %d: %v", a.cfg.Name, i, err)
			return err
		}
	}

	log.Printf("[%s] Application started successfully. Press Ctrl+C to exit.", a.cfg.Name)

	// Wait for termination signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	sig := <-quit
	log.Printf("[%s] Received signal %v, initiating graceful shutdown...", a.cfg.Name, sig)

	// Execute all shutdown functions (LIFO order)
	a.shutdown.Execute()

	log.Printf("[%s] Application stopped gracefully.", a.cfg.Name)
	return nil
}
