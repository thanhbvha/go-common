// Package graceful provides utilities for graceful shutdown of services.
//
// Preferred usage — struct-based, testable, supports multiple instances:
//
//	sd := graceful.NewShutdown(30 * time.Second)
//	sd.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
//	sd.Wait()
//
// Legacy usage — package-level functions kept for backward compatibility:
//
//	graceful.Register(func(ctx context.Context) error { ... })
//	graceful.Wait(30 * time.Second)
package graceful

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Shutdown coordinates graceful shutdown for a set of cleanup functions.
// It is safe for concurrent use and supports multiple independent instances.
type Shutdown struct {
	mu      sync.Mutex
	funcs   []func(ctx context.Context) error
	timeout time.Duration
}

// NewShutdown creates a new Shutdown coordinator with the given cleanup timeout.
// A timeout <= 0 defaults to 30 seconds.
func NewShutdown(timeout time.Duration) *Shutdown {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Shutdown{timeout: timeout}
}

// Register adds a cleanup function to be executed during shutdown.
// Functions are executed in LIFO order (last registered = first to run).
func (s *Shutdown) Register(fn func(ctx context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.funcs = append([]func(ctx context.Context) error{fn}, s.funcs...)
}

// Wait blocks until SIGINT or SIGTERM is received, then executes all cleanup functions.
func (s *Shutdown) Wait() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Printf("[Graceful] Received signal: %v. Starting graceful shutdown...", sig)
	s.Execute()
}

// Execute runs all cleanup functions immediately (without waiting for a signal).
// Useful for programmatic shutdown or testing.
func (s *Shutdown) Execute() {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	s.mu.Lock()
	funcs := make([]func(ctx context.Context) error, len(s.funcs))
	copy(funcs, s.funcs)
	s.mu.Unlock()

	for i, fn := range funcs {
		log.Printf("[Graceful] Running cleanup task %d/%d...", i+1, len(funcs))
		if err := fn(ctx); err != nil {
			log.Printf("[Graceful] Error in cleanup task %d: %v", i+1, err)
		}
	}
	log.Println("[Graceful] Shutdown completed.")
}

// ---- Package-level API (legacy, backward-compatible) ----

var globalShutdown = NewShutdown(30 * time.Second)

// Register adds a cleanup function to the global Shutdown instance.
//
// Deprecated: Use graceful.NewShutdown(timeout).Register(fn) for a testable,
// struct-based approach. This global function will be removed in a future version.
func Register(fn func(ctx context.Context) error) {
	globalShutdown.Register(fn)
}

// Wait blocks until an OS signal is received, then executes all registered
// cleanup functions with the specified timeout.
//
// Deprecated: Use graceful.NewShutdown(timeout).Wait() instead.
func Wait(timeout time.Duration) {
	globalShutdown.mu.Lock()
	globalShutdown.timeout = timeout
	globalShutdown.mu.Unlock()
	globalShutdown.Wait()
}
