package main

import (
	"context"
	"errors"
	"time"

	"github.com/thanhbvha/go-common/logger"
)

func main() {
	// ==========================================
	// 1. INITIALIZATION (Required on Startup)
	// ==========================================
	// The logger module provides a high-performance, asynchronous logger.
	// You should initialize it once and set it as the global default.
	
	opts := logger.DefaultOptions()
	opts.Level = -4 // DEBUG level (slog.LevelDebug is -4)
	opts.StdOut = true
	opts.TextFormat = true // Use Text format instead of JSON for local development

	// Example: Configure file rotation (optional)
	// opts.File = &logger.FileOptions{
	// 	Path:       "logs/app.log",
	// 	MaxSizeMB:  10,
	// 	MaxBackups: 5,
	// 	MaxAgeDays: 30,
	// }

	l := logger.New(opts)
	
	// Set it as the process-wide default so you can use logger.Info() anywhere
	logger.SetDefault(l)

	// ALWAYS call Close() before the app exits to flush all buffered async logs
	defer logger.Close()

	// ==========================================
	// 2. SYNCHRONOUS LOGGING
	// ==========================================
	// Synchronous logging blocks the current goroutine until the log is written.
	logger.Info("This is a synchronous INFO log", "module", "main")
	logger.Debug("This is a synchronous DEBUG log", "user_id", 123)

	// ==========================================
	// 3. ASYNCHRONOUS LOGGING (Recommended for high-throughput)
	// ==========================================
	// Async logging pushes the log to a buffered channel and returns immediately.
	// It is highly recommended for API handlers to avoid I/O bottlenecks.
	logger.InfoAsync("This is an async INFO log", "module", "main", "action", "async_test")
	
	err := errors.New("simulated db timeout")
	logger.ErrorAsync("Database query failed", "error", err, "query_id", "Q-1234")

	// ==========================================
	// 4. CONTEXT-AWARE LOGGING (Tracing)
	// ==========================================
	// If your context contains a RequestID (usually injected by a middleware),
	// the logger can automatically extract and append it to the log fields.
	
	// Simulate a context with a request ID
	ctx := context.WithValue(context.Background(), logger.ContextKeyRequestID, "REQ-9999")

	// Use the *WithContext variants
	logger.InfoWithContext(ctx, "Processing payment request", "amount", 500)
	logger.ErrorWithContext(ctx, "Payment failed", "reason", "insufficient_funds")

	// Wait a moment before exit to let async logs print (in real apps, the HTTP server blocks)
	time.Sleep(100 * time.Millisecond)
	
	logger.Info("Shutting down logger example")
}
