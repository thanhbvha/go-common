package graceful_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/utils/graceful"
)

func TestShutdownExecutesHooksOnceInLIFOOrder(t *testing.T) {
	var order []int
	shutdown := graceful.NewShutdown(time.Second)
	shutdown.Register(func(context.Context) error { order = append(order, 1); return nil })
	shutdown.Register(func(context.Context) error { order = append(order, 2); return nil })

	if err := shutdown.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if err := shutdown.Execute(); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if !reflect.DeepEqual(order, []int{2, 1}) {
		t.Fatalf("execution order = %v", order)
	}
}

func TestShutdownJoinsErrorsAndRecoversPanics(t *testing.T) {
	errClose := errors.New("close failed")
	shutdown := graceful.NewShutdown(time.Second)
	shutdown.Register(func(context.Context) error { return errClose })
	shutdown.Register(func(context.Context) error { panic("broken hook") })

	err := shutdown.Execute()
	if !errors.Is(err, errClose) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestShutdownSharesTimeout(t *testing.T) {
	shutdown := graceful.NewShutdown(10 * time.Millisecond)
	shutdown.Register(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err := shutdown.Execute(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestWaitContextRunsOnCancellation(t *testing.T) {
	var called atomic.Bool
	shutdown := graceful.NewShutdown(time.Second)
	shutdown.Register(func(context.Context) error { called.Store(true); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := shutdown.WaitContext(ctx); err != nil {
		t.Fatalf("WaitContext() error = %v", err)
	}
	if !called.Load() {
		t.Fatal("hook was not called")
	}
}

func TestConcurrentExecuteRunsHooksOnce(t *testing.T) {
	var calls atomic.Int32
	shutdown := graceful.NewShutdown(time.Second)
	shutdown.Register(func(context.Context) error {
		calls.Add(1)
		return nil
	})

	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() {
			if err := shutdown.Execute(); err != nil {
				t.Errorf("Execute() error = %v", err)
			}
		})
	}
	callers.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("hook calls = %d", got)
	}
}

func TestWaitContextReturnsAfterProgrammaticExecution(t *testing.T) {
	shutdown := graceful.NewShutdown(time.Second)
	if err := shutdown.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if err := shutdown.WaitContext(context.Background()); err != nil {
		t.Fatalf("WaitContext() error = %v", err)
	}
}
