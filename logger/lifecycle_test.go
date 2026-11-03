package logger

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

type contextValueHandler struct {
	key  any
	want any
	seen chan bool
}

func (h *contextValueHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *contextValueHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.seen <- ctx.Value(h.key) == h.want
	return nil
}
func (h *contextValueHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *contextValueHandler) WithGroup(string) slog.Handler      { return h }

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedBuffer) text() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

func TestRuntimeOwnsDerivedLoggersAndRejectsAfterShutdown(t *testing.T) {
	b := &lockedBuffer{}
	l := New(Options{Writer: b, Async: true, BufferSize: 8, NumWorkers: 2})
	child := l.With("component", "child")
	child.Info("before shutdown")
	if err := l.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := b.text(); !strings.Contains(got, "before shutdown") || !strings.Contains(got, "child") {
		t.Fatal(got)
	}
	before := b.text()
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "after shutdown", 0)
	if err := child.Handler().Handle(t.Context(), r); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if b.text() != before {
		t.Fatal("write after shutdown")
	}
	if err := l.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type blockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockedWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return len(p), nil
}
func TestShutdownDeadlineAndConcurrentLogging(t *testing.T) {
	b := &blockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	l := New(Options{Writer: b, Async: true, BufferSize: 1})
	l.Info("blocked")
	<-b.entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := l.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	close(b.release)
	if err := l.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestSynchronousDefaultAndConcurrentShutdown(t *testing.T) {
	b := &lockedBuffer{}
	l := New(Options{Writer: b})
	l.Info("visible immediately")
	if !strings.Contains(b.text(), "visible immediately") {
		t.Fatal("default is not synchronous")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { l.Info("racing"); _ = l.Shutdown(t.Context()) })
	}
	wg.Wait()
}

func TestContextMethodsPassContextToHandler(t *testing.T) {
	type contextKey struct{}
	handler := &contextValueHandler{key: contextKey{}, want: "value", seen: make(chan bool, 1)}
	state := &runtimeState{done: make(chan struct{})}
	runtime := &Logger{sl: slog.New(&runtimeHandler{base: handler, state: state}), state: state}
	ctx := context.WithValue(t.Context(), contextKey{}, "value")

	runtime.InfoWithContext(ctx, "message")
	if !<-handler.seen {
		t.Fatal("context value did not reach slog handler")
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}
