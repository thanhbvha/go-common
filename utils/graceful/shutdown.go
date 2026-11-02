// Package graceful coordinates one-shot, LIFO service shutdown.
package graceful

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Hook releases one service resource. Hooks should stop promptly when ctx is
// canceled and should return any incomplete cleanup as an error.
type Hook func(ctx context.Context) error

// Shutdown is a concurrency-safe, one-shot shutdown coordinator.
type Shutdown struct {
	mu      sync.Mutex
	hooks   []Hook
	timeout time.Duration
	started bool
	once    sync.Once
	err     error
	done    chan struct{}
}

// NewShutdown creates a coordinator. It panics for a non-positive timeout,
// which is a startup configuration error.
func NewShutdown(timeout time.Duration) *Shutdown {
	if timeout <= 0 {
		panic("graceful: timeout must be positive")
	}
	return &Shutdown{timeout: timeout, done: make(chan struct{})}
}

// Register adds a non-nil hook. Hooks execute in reverse registration order.
// Register panics when hook is nil or shutdown has already started.
func (s *Shutdown) Register(hook Hook) {
	if hook == nil {
		panic("graceful: hook must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		panic("graceful: cannot register a hook after shutdown has started")
	}
	s.hooks = append(s.hooks, hook)
}

// Wait waits for SIGINT or SIGTERM and then executes the registered hooks.
func (s *Shutdown) Wait() error {
	return s.WaitContext(context.Background())
}

// WaitContext waits for SIGINT, SIGTERM, or ctx cancellation and then executes
// the registered hooks. Signal resources are released before it returns.
func (s *Shutdown) WaitContext(ctx context.Context) error {
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-signalCtx.Done():
	case <-s.done:
	}
	return s.Execute()
}

// Execute runs all hooks once with the configured shared timeout. Concurrent or
// repeated calls wait for the first execution and return the same joined error.
func (s *Shutdown) Execute() error {
	s.once.Do(func() {
		defer close(s.done)
		s.mu.Lock()
		s.started = true
		hooks := append([]Hook(nil), s.hooks...)
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()

		var hookErrors []error
		for i := len(hooks) - 1; i >= 0; i-- {
			if err := executeHook(ctx, hooks[i]); err != nil {
				hookErrors = append(hookErrors, fmt.Errorf("graceful: hook %d: %w", i, err))
			}
		}
		s.err = errors.Join(hookErrors...)
	})
	return s.err
}

func executeHook(ctx context.Context, hook Hook) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return hook(ctx)
}
