package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartitionedExecutorPreservesKeyOrder(t *testing.T) {
	executor := NewPartitionedExecutor(4, 32)
	t.Cleanup(func() { _ = executor.Shutdown(context.Background()) })
	var mu sync.Mutex
	order := make([]int, 0, 10)
	for i := 0; i < 10; i++ {
		i := i
		if err := executor.Submit(context.Background(), "connection-1", func(context.Context) error {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := executor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, value := range order {
		if value != i {
			t.Fatalf("order[%d] = %d", i, value)
		}
	}
}

func TestPartitionedExecutorBoundsConcurrency(t *testing.T) {
	executor := NewPartitionedExecutor(2, 16)
	gate := make(chan struct{})
	var running atomic.Int32
	var maximum atomic.Int32
	for _, key := range []string{"a", "b"} {
		if err := executor.Submit(context.Background(), key, func(context.Context) error {
			current := running.Add(1)
			for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
			}
			<-gate
			running.Add(-1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	if maximum.Load() > 2 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
	close(gate)
	if err := executor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionedExecutorQueueFullAndStopped(t *testing.T) {
	executor := NewPartitionedExecutor(1, 1)
	gate := make(chan struct{})
	started := make(chan struct{})
	if err := executor.Submit(context.Background(), "key", func(context.Context) error { close(started); <-gate; return nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := executor.Submit(context.Background(), "key", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := executor.Submit(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, ErrExecutorFull) {
		t.Fatalf("queue-full error = %v", err)
	}
	close(gate)
	if err := executor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.Submit(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, ErrExecutorStopped) {
		t.Fatalf("stopped error = %v", err)
	}
}
