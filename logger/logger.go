// Package logger provides a structured, asynchronous logger built on top of
// the standard library's log/slog. Log records are dispatched to a pool of
// worker goroutines through a buffered channel, so callers are never blocked
// by I/O. When the channel is full, the logger degrades gracefully to
// synchronous writes rather than dropping records.
//
// Optional file rotation is available via lumberjack when FileOptions is set
// in Options. Fiber (or any other HTTP framework) integration is intentionally
// not included; framework-specific middleware lives in a separate adapter repo.
//
// Basic usage:
//
//	l := logger.New(logger.DefaultOptions())
//	logger.SetDefault(l)
//	defer logger.Close()
//
//	logger.Info("server started", "port", 8080)
//	logger.ErrorAsync("handler failed", "err", err)
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// ContextKey is the unexported type for context keys managed by this package.
// Use the exported constants below to store and retrieve values.
type ContextKey string

// ContextKeyRequestID is the context key under which a request/trace ID is stored.
// Use context.WithValue(ctx, logger.ContextKeyRequestID, id) to attach an ID,
// then call the *WithContext variants to have it appended automatically.
const ContextKeyRequestID ContextKey = "request_id"

// asyncTask is a unit of deferred log work dispatched to a worker goroutine.
type asyncTask func()

// Logger is a structured, asynchronous logger backed by log/slog.
// It is safe for concurrent use. Zero value is not usable; create with New.
type Logger struct {
	sl      *slog.Logger
	logChan chan asyncTask
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closer  io.Closer // non-nil when writing to a rotating file
}

// package-level default logger, guarded by defaultMu.
var (
	defaultLogger *Logger
	defaultMu     sync.RWMutex
)

// New creates and starts a Logger configured by opts.
// Workers are launched immediately; call Close to stop them and flush buffered
// entries before the process exits.
func New(opts Options) *Logger {
	numWorkers := opts.NumWorkers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 5
		if numWorkers < 1 {
			numWorkers = 1
		}
	}

	bufSize := opts.BufferSize
	if bufSize <= 0 {
		bufSize = 100_000
	}

	// Build the io.Writer list.
	var writers []io.Writer
	var closer io.Closer

	if opts.File != nil && opts.File.Path != "" {
		maxSize := opts.File.MaxSizeMB
		if maxSize <= 0 {
			maxSize = 10
		}
		maxBackups := opts.File.MaxBackups
		if maxBackups <= 0 {
			maxBackups = 5
		}
		maxAge := opts.File.MaxAgeDays
		if maxAge <= 0 {
			maxAge = 30
		}
		lj := &lumberjack.Logger{
			Filename:   opts.File.Path,
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
			MaxAge:     maxAge,
			Compress:   opts.File.Compress,
		}
		writers = append(writers, lj)
		closer = lj
	}

	if opts.StdOut || len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	var w io.Writer
	if len(writers) == 1 {
		w = writers[0]
	} else {
		w = io.MultiWriter(writers...)
	}

	var handler slog.Handler
	if opts.TextFormat {
		handler = slog.NewTextHandler(w, &slog.HandlerOptions{Level: opts.Level})
	} else {
		handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: opts.Level})
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &Logger{
		sl:      slog.New(handler),
		logChan: make(chan asyncTask, bufSize),
		cancel:  cancel,
		closer:  closer,
	}

	for i := 0; i < numWorkers; i++ {
		l.wg.Add(1)
		go l.runWorker(ctx)
	}

	return l
}

// runWorker drains the async channel until ctx is cancelled, then flushes any
// remaining entries before returning.
func (l *Logger) runWorker(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case fn := <-l.logChan:
			if fn != nil {
				fn()
			}
		case <-ctx.Done():
			// Flush remaining buffered entries before exiting.
			for {
				select {
				case fn := <-l.logChan:
					if fn != nil {
						fn()
					}
				default:
					return
				}
			}
		}
	}
}

// dispatch sends fn to the async channel. Falls back to synchronous execution
// when the channel is at capacity to prevent record loss under high load.
func (l *Logger) dispatch(fn asyncTask) {
	select {
	case l.logChan <- fn:
	default:
		fn()
	}
}

// SetDefault registers l as the process-wide default Logger used by all
// package-level functions (Info, Error, WarnAsync, …).
// It is safe to call from multiple goroutines; the last caller wins.
func SetDefault(l *Logger) {
	defaultMu.Lock()
	defaultLogger = l
	defaultMu.Unlock()
}

// Default returns the current process-wide default Logger.
// Returns nil if SetDefault has not been called yet.
func Default() *Logger {
	defaultMu.RLock()
	l := defaultLogger
	defaultMu.RUnlock()
	return l
}

// getDefault is an internal helper used by package-level log functions.
func getDefault() *Logger {
	defaultMu.RLock()
	l := defaultLogger
	defaultMu.RUnlock()
	return l
}

// Close gracefully shuts down the Logger, waiting up to 5 seconds for all
// buffered async entries to be flushed before closing any file writer.
// Subsequent calls are safe but have no effect.
func (l *Logger) Close() {
	if l.cancel != nil {
		l.cancel()
	}

	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		slog.Warn("logger: shutdown timed out, some buffered entries may have been lost")
	}

	if l.closer != nil {
		_ = l.closer.Close()
	}
}

// ---- Synchronous methods ----

// Info logs a message at INFO level synchronously on this Logger.
func (l *Logger) Info(msg string, args ...any) { l.sl.Info(msg, args...) }

// Error logs a message at ERROR level synchronously on this Logger.
func (l *Logger) Error(msg string, args ...any) { l.sl.Error(msg, args...) }

// Warn logs a message at WARN level synchronously on this Logger.
func (l *Logger) Warn(msg string, args ...any) { l.sl.Warn(msg, args...) }

// Debug logs a message at DEBUG level synchronously on this Logger.
func (l *Logger) Debug(msg string, args ...any) { l.sl.Debug(msg, args...) }

// ---- Async methods ----

// InfoAsync enqueues a message at INFO level for asynchronous delivery.
func (l *Logger) InfoAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Info(msg, args...) })
}

// ErrorAsync enqueues a message at ERROR level for asynchronous delivery.
func (l *Logger) ErrorAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Error(msg, args...) })
}

// WarnAsync enqueues a message at WARN level for asynchronous delivery.
func (l *Logger) WarnAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Warn(msg, args...) })
}

// DebugAsync enqueues a message at DEBUG level for asynchronous delivery.
func (l *Logger) DebugAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Debug(msg, args...) })
}

// ---- Context-aware methods ----

// extractRequestID reads the request ID stored in ctx under ContextKeyRequestID.
func extractRequestID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ContextKeyRequestID).(string)
	return id, ok && id != ""
}

// InfoWithContext logs at INFO level, appending request_id from ctx when present.
func (l *Logger) InfoWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.Info(msg, args...)
}

// ErrorWithContext logs at ERROR level, appending request_id from ctx when present.
func (l *Logger) ErrorWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.Error(msg, args...)
}

// WarnWithContext logs at WARN level, appending request_id from ctx when present.
func (l *Logger) WarnWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.Warn(msg, args...)
}

// DebugWithContext logs at DEBUG level, appending request_id from ctx when present.
func (l *Logger) DebugWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.Debug(msg, args...)
}

// ---- Package-level functions (delegate to default Logger) ----

// fallback executes fn on the default logger, or calls fallbackFn when no
// default has been registered.
func fallback(fn func(*Logger), fallbackFn func()) {
	if l := getDefault(); l != nil {
		fn(l)
	} else {
		fallbackFn()
	}
}

// InfoWithContext logs at INFO level using the default Logger, extracting request_id from ctx.
func InfoWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.InfoWithContext(ctx, msg, args...) }, func() { slog.Info(msg, args...) })
}

// ErrorWithContext logs at ERROR level using the default Logger, extracting request_id from ctx.
func ErrorWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.ErrorWithContext(ctx, msg, args...) }, func() { slog.Error(msg, args...) })
}

// WarnWithContext logs at WARN level using the default Logger, extracting request_id from ctx.
func WarnWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.WarnWithContext(ctx, msg, args...) }, func() { slog.Warn(msg, args...) })
}

// DebugWithContext logs at DEBUG level using the default Logger, extracting request_id from ctx.
func DebugWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.DebugWithContext(ctx, msg, args...) }, func() { slog.Debug(msg, args...) })
}

// Info logs at INFO level using the default Logger.
func Info(msg string, args ...any) {
	fallback(func(l *Logger) { l.sl.Info(msg, args...) }, func() { slog.Info(msg, args...) })
}

// Error logs at ERROR level using the default Logger.
func Error(msg string, args ...any) {
	fallback(func(l *Logger) { l.sl.Error(msg, args...) }, func() { slog.Error(msg, args...) })
}

// Warn logs at WARN level using the default Logger.
func Warn(msg string, args ...any) {
	fallback(func(l *Logger) { l.sl.Warn(msg, args...) }, func() { slog.Warn(msg, args...) })
}

// Debug logs at DEBUG level using the default Logger.
func Debug(msg string, args ...any) {
	fallback(func(l *Logger) { l.sl.Debug(msg, args...) }, func() { slog.Debug(msg, args...) })
}

// InfoAsync enqueues at INFO level on the default Logger (sync fallback if unset).
func InfoAsync(msg string, args ...any) {
	fallback(func(l *Logger) { l.InfoAsync(msg, args...) }, func() { slog.Info(msg, args...) })
}

// ErrorAsync enqueues at ERROR level on the default Logger (sync fallback if unset).
func ErrorAsync(msg string, args ...any) {
	fallback(func(l *Logger) { l.ErrorAsync(msg, args...) }, func() { slog.Error(msg, args...) })
}

// WarnAsync enqueues at WARN level on the default Logger (sync fallback if unset).
func WarnAsync(msg string, args ...any) {
	fallback(func(l *Logger) { l.WarnAsync(msg, args...) }, func() { slog.Warn(msg, args...) })
}

// DebugAsync enqueues at DEBUG level on the default Logger (sync fallback if unset).
func DebugAsync(msg string, args ...any) {
	fallback(func(l *Logger) { l.DebugAsync(msg, args...) }, func() { slog.Debug(msg, args...) })
}

// Close shuts down the default Logger. Safe to call even if SetDefault was
// never called.
func Close() {
	if l := getDefault(); l != nil {
		l.Close()
	}
}
