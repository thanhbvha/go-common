// Package logger provides structured slog logging with explicitly owned lifecycle.
// Logging is synchronous by default. Optional async logging uses a bounded queue.
package logger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	"gopkg.in/natefinch/lumberjack.v2"
)

type ContextKey string

const ContextKeyRequestID ContextKey = "request_id"

var ErrClosed = errors.New("logger: runtime closed")
var ErrDropped = errors.New("logger: async queue full")

// Logger owns its handler, workers and any file writer. Child slog loggers only
// add fields and cannot close this runtime. Writers supplied by callers are borrowed.
type Logger struct {
	sl    *slog.Logger
	state *runtimeState
}
type queuedRecord struct {
	ctx     context.Context
	record  slog.Record
	handler slog.Handler
}
type runtimeState struct {
	mu       sync.Mutex
	closed   bool
	queue    chan queuedRecord
	workers  sync.WaitGroup
	writes   sync.WaitGroup
	done     chan struct{}
	closer   io.Closer
	err      error
	overflow OverflowPolicy
	onError  func(error)
}
type runtimeHandler struct {
	base  slog.Handler
	state *runtimeState
}

func (h *runtimeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.base.Enabled(ctx, l)
}
func (h *runtimeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &runtimeHandler{h.base.WithAttrs(attrs), h.state}
}
func (h *runtimeHandler) WithGroup(name string) slog.Handler {
	return &runtimeHandler{h.base.WithGroup(name), h.state}
}
func (h *runtimeHandler) Handle(ctx context.Context, r slog.Record) error {
	s := h.state
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.queue != nil {
		select {
		case s.queue <- queuedRecord{context.WithoutCancel(ctx), r.Clone(), h.base}:
			s.mu.Unlock()
			return nil
		default:
		}
		if s.overflow == OverflowDrop {
			s.mu.Unlock()
			s.report(ErrDropped)
			return ErrDropped
		}
	}
	s.writes.Add(1)
	s.mu.Unlock()
	defer s.writes.Done()
	err := h.base.Handle(ctx, r)
	s.report(err)
	return err
}
func (s *runtimeState) report(err error) {
	if err != nil && s.onError != nil {
		func() { defer func() { _ = recover() }(); s.onError(err) }()
	}
}

var (
	defaultLogger *Logger
	defaultMu     sync.RWMutex
)

// New creates a logging runtime. Async workers are started only when Async is true.
// Shutdown must run after producers stop. OnError must return promptly.
func New(opts Options) *Logger {
	var writers []io.Writer
	var closer io.Closer
	if opts.Writer != nil {
		writers = append(writers, opts.Writer)
	}
	if opts.File != nil && opts.File.Path != "" {
		f := *opts.File
		if f.MaxSizeMB <= 0 {
			f.MaxSizeMB = 10
		}
		if f.MaxBackups <= 0 {
			f.MaxBackups = 5
		}
		if f.MaxAgeDays <= 0 {
			f.MaxAgeDays = 30
		}
		lj := &lumberjack.Logger{Filename: f.Path, MaxSize: f.MaxSizeMB, MaxBackups: f.MaxBackups, MaxAge: f.MaxAgeDays, Compress: f.Compress}
		writers = append(writers, lj)
		closer = lj
	}
	if opts.StdOut || len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}
	var writer io.Writer = writers[0]
	if len(writers) > 1 {
		writer = io.MultiWriter(writers...)
	}
	var handler slog.Handler
	ho := &slog.HandlerOptions{Level: opts.Level}
	if opts.TextFormat {
		handler = slog.NewTextHandler(writer, ho)
	} else {
		handler = slog.NewJSONHandler(writer, ho)
	}
	s := &runtimeState{done: make(chan struct{}), closer: closer, overflow: opts.Overflow, onError: opts.OnError}
	if opts.Async {
		size := opts.BufferSize
		if size <= 0 {
			size = 1024
		}
		workers := opts.NumWorkers
		if workers <= 0 {
			workers = 1
		}
		s.queue = make(chan queuedRecord, size)
		for range workers {
			s.workers.Go(func() {
				for item := range s.queue {
					s.report(item.handler.Handle(item.ctx, item.record))
				}
			})
		}
	}
	return &Logger{sl: slog.New(&runtimeHandler{handler, s}), state: s}
}

// Logger returns the native slog logger; derived loggers share runtime admission.
func (l *Logger) Logger() *slog.Logger          { return l.sl }
func (l *Logger) With(args ...any) *slog.Logger { return l.sl.With(args...) }
func (l *Logger) dispatch(fn func())            { fn() }

// Shutdown stops admission, drains accepted writes, then closes an owned writer.
// A caller deadline bounds waiting; cleanup continues without closing a writer
// underneath a blocked write. Calling Shutdown concurrently is safe.
func (l *Logger) Shutdown(ctx context.Context) error {
	s := l.state
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.queue != nil {
			close(s.queue)
		}
		go func() {
			s.workers.Wait()
			s.writes.Wait()
			if s.closer != nil {
				s.err = s.closer.Close()
			}
			close(s.done)
		}()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return s.err
	default:
	}
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close is a five-second convenience shutdown. Use Shutdown to observe errors.
func (l *Logger) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = l.Shutdown(ctx)
}
func SetDefault(l *Logger) { defaultMu.Lock(); defaultLogger = l; defaultMu.Unlock() }
func Default() *Logger     { defaultMu.RLock(); defer defaultMu.RUnlock(); return defaultLogger }
func getDefault() *Logger  { return Default() }

// ---- Methods using the configured synchronous or async handler ----

// Info logs a message at INFO level using the configured handler.
func (l *Logger) Info(msg string, args ...any) { l.sl.Info(msg, args...) }

// Error logs a message at ERROR level using the configured handler.
func (l *Logger) Error(msg string, args ...any) { l.sl.Error(msg, args...) }

// Warn logs a message at WARN level using the configured handler.
func (l *Logger) Warn(msg string, args ...any) { l.sl.Warn(msg, args...) }

// Debug logs a message at DEBUG level using the configured handler.
func (l *Logger) Debug(msg string, args ...any) { l.sl.Debug(msg, args...) }

// ---- Async methods ----

// InfoAsync logs at INFO using the configured handler.
func (l *Logger) InfoAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Info(msg, args...) })
}

// ErrorAsync logs at ERROR using the configured handler.
func (l *Logger) ErrorAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Error(msg, args...) })
}

// WarnAsync logs at WARN using the configured handler.
func (l *Logger) WarnAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Warn(msg, args...) })
}

// DebugAsync logs at DEBUG using the configured handler.
func (l *Logger) DebugAsync(msg string, args ...any) {
	l.dispatch(func() { l.sl.Debug(msg, args...) })
}

// ---- Context-aware methods ----

// extractRequestID prefers the shared ctxkey value and falls back to the legacy
// logger-specific key for backward compatibility.
func extractRequestID(ctx context.Context) (string, bool) {
	if id, ok := ctxkey.GetRequestID(ctx); ok && id != "" {
		return id, true
	}
	id, ok := ctx.Value(ContextKeyRequestID).(string)
	return id, ok && id != ""
}

// InfoWithContext logs at INFO level, appending request_id from ctx when present.
func (l *Logger) InfoWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.InfoContext(ctx, msg, args...)
}

// ErrorWithContext logs at ERROR level, appending request_id from ctx when present.
func (l *Logger) ErrorWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.ErrorContext(ctx, msg, args...)
}

// WarnWithContext logs at WARN level, appending request_id from ctx when present.
func (l *Logger) WarnWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.WarnContext(ctx, msg, args...)
}

// DebugWithContext logs at DEBUG level, appending request_id from ctx when present.
func (l *Logger) DebugWithContext(ctx context.Context, msg string, args ...any) {
	if id, ok := extractRequestID(ctx); ok {
		args = append(args, "request_id", id)
	}
	l.sl.DebugContext(ctx, msg, args...)
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
	fallback(func(l *Logger) { l.InfoWithContext(ctx, msg, args...) }, func() { slog.InfoContext(ctx, msg, args...) })
}

// ErrorWithContext logs at ERROR level using the default Logger, extracting request_id from ctx.
func ErrorWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.ErrorWithContext(ctx, msg, args...) }, func() { slog.ErrorContext(ctx, msg, args...) })
}

// WarnWithContext logs at WARN level using the default Logger, extracting request_id from ctx.
func WarnWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.WarnWithContext(ctx, msg, args...) }, func() { slog.WarnContext(ctx, msg, args...) })
}

// DebugWithContext logs at DEBUG level using the default Logger, extracting request_id from ctx.
func DebugWithContext(ctx context.Context, msg string, args ...any) {
	fallback(func(l *Logger) { l.DebugWithContext(ctx, msg, args...) }, func() { slog.DebugContext(ctx, msg, args...) })
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

// With returns a new Logger derived from the default Logger with the given
// key-value pairs pre-attached. Falls back to a new standard logger if default is unset.
func With(args ...any) *slog.Logger {
	if l := getDefault(); l != nil {
		return l.With(args...)
	}
	return slog.Default().With(args...)
}
