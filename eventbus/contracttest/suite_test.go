package contracttest

import (
	"context"
	"errors"
	"runtime/pprof"
	"testing"
	"time"
)

func TestSessionQuiescenceDetectsRetainedGoroutine(t *testing.T) {
	label := "test-retained-session"
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go pprof.Do(context.Background(), pprof.Labels(sessionGoroutineLabel, label), func(context.Context) { defer close(done); close(started); <-release })
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := waitSessionGoroutines(ctx, label)
	cancel()
	close(release)
	<-done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retained session not detected: %v", err)
	}
	cleanup, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = waitSessionGoroutines(cleanup, label); err != nil {
		t.Fatalf("released session not quiescent: %v", err)
	}
}
