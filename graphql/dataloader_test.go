package graphql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDataLoaderContract(t *testing.T) {
	var calls atomic.Int64
	l, err := NewDataLoader(context.Background(), func(_ context.Context, keys []int) (map[int]int, error) {
		calls.Add(1)
		out := map[int]int{}
		for _, k := range keys {
			if k != 9 {
				out[k] = k * 10
			}
		}
		return out, nil
	}, LoaderConfig{MaxBatch: 2})
	if err != nil {
		t.Fatal(err)
	}
	values, errs := l.LoadAll(context.Background(), []int{2, 1, 2, 9})
	if values[0] != 20 || values[1] != 10 || values[2] != 20 || !errors.Is(errs[3], ErrNotFound) || calls.Load() != 2 {
		t.Fatal(values, errs, calls.Load())
	}
	_, _ = l.Load(context.Background(), 9)
	if calls.Load() != 2 {
		t.Fatal("missing key not cached")
	}
	if l.Prime(2, 22) {
		t.Fatal("prime overwrote cache")
	}
	l.Clear(2)
	if !l.Prime(2, 22) {
		t.Fatal("prime failed")
	}
	if v, _ := l.Load(context.Background(), 2); v != 22 {
		t.Fatal(v)
	}
	l.ClearAll()
	if v, _ := l.Load(context.Background(), 2); v != 20 {
		t.Fatal(v)
	}
}
func TestLoaderBudgetAndValidation(t *testing.T) {
	fn := func(context.Context, []int) (map[int]int, error) { return map[int]int{1: 1}, nil }
	if _, err := NewDataLoader[int, int](nil, fn, LoaderConfig{}); err == nil {
		t.Fatal("nil ctx")
	}
	if _, err := NewDataLoader[int, int](context.Background(), nil, LoaderConfig{}); err == nil {
		t.Fatal("nil fn")
	}
	for _, cfg := range []LoaderConfig{{Wait: -1}, {MaxBatch: -1}, {MaxKeys: -1}, {MaxConcurrentBatches: -1}} {
		if _, err := NewDataLoader(context.Background(), fn, cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	l, _ := NewDataLoader(context.Background(), fn, LoaderConfig{MaxKeys: 1})
	_, _ = l.Load(context.Background(), 1)
	l.ClearAll()
	if _, err := l.Load(context.Background(), 1); !errors.Is(err, ErrLoaderLimit) {
		t.Fatal(err)
	}
	if l.Prime(2, 2) {
		t.Fatal("prime bypassed budget")
	}
}
func TestLoaderBatchErrorAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		var calls atomic.Int64
		failure := errors.New("backend unavailable")
		l, _ := NewDataLoader(context.Background(), func(context.Context, []int) (map[int]int, error) {
			calls.Add(1)
			if panics {
				panic("secret")
			}
			return nil, failure
		}, LoaderConfig{})
		for i := 0; i < 2; i++ {
			_, err := l.Load(context.Background(), 1)
			if err == nil || (panics && err.Error() != "[INTERNAL_ERROR] Internal Server Error") {
				t.Fatal(err)
			}
		}
		if calls.Load() != 1 {
			t.Fatal(calls.Load())
		}
		l.Clear(1)
		_, _ = l.Load(context.Background(), 1)
		if calls.Load() != 2 {
			t.Fatal("retry failed")
		}
	}
}
func TestLoaderContextAndConcurrency(t *testing.T) {
	type identityKey struct{}
	requestCtx, cancel := context.WithCancel(context.WithValue(context.Background(), identityKey{}, "request"))
	defer cancel()
	var current, peak atomic.Int64
	l, _ := NewDataLoader(requestCtx, func(ctx context.Context, keys []int) (map[int]int, error) {
		if ctx.Value(identityKey{}) != "request" {
			t.Error("wrong batch context")
		}
		n := current.Add(1)
		defer current.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		return map[int]int{keys[0]: keys[0]}, nil
	}, LoaderConfig{MaxBatch: 1, MaxConcurrentBatches: 2})
	caller, cancelCaller := context.WithCancel(context.Background())
	thunk := l.enqueue(caller, 1)
	cancelCaller()
	if _, err := thunk(); err != nil {
		t.Fatal("caller cancellation poisoned request batch", err)
	}
	keys := []int{2, 3, 4, 5, 6, 7}
	_, errs := l.LoadAll(context.Background(), keys)
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if peak.Load() > 2 {
		t.Fatal(peak.Load())
	}
	cancel()
	if _, err := l.Load(context.Background(), 8); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestLoaderConcurrentDedup(t *testing.T) {
	var calls atomic.Int64
	l, _ := NewDataLoader(context.Background(), func(context.Context, []int) (map[int]int, error) { calls.Add(1); return map[int]int{1: 1}, nil }, LoaderConfig{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := l.Load(context.Background(), 1); err != nil || v != 1 {
				t.Error(v, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
