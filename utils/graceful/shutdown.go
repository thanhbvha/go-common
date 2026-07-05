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

type cleanupFunc func(ctx context.Context) error

var (
	cleanupFuncs []cleanupFunc
	mu           sync.Mutex
)

// Register adds a cleanup function to be executed during shutdown.
// The functions will be executed in reverse order (LIFO).
func Register(fn func(ctx context.Context) error) {
	mu.Lock()
	defer mu.Unlock()
	cleanupFuncs = append([]cleanupFunc{fn}, cleanupFuncs...) // Prepend for LIFO
}

// Wait blocks until an OS interrupt signal is received, then executes
// all registered cleanup functions with the specified timeout.
func Wait(timeout time.Duration) {
	// Create channel to listen for OS signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Block until a signal is received
	sig := <-quit
	log.Printf("[Graceful] Received signal: %v. Starting graceful shutdown...", sig)

	// Create a context with timeout for cleanup operations
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	mu.Lock()
	defer mu.Unlock()

	// Execute cleanup functions
	for i, fn := range cleanupFuncs {
		log.Printf("[Graceful] Running cleanup task %d/%d...", i+1, len(cleanupFuncs))
		if err := fn(ctx); err != nil {
			log.Printf("[Graceful] Error in cleanup task %d: %v", i+1, err)
		}
	}

	log.Println("[Graceful] Shutdown completed.")
}
