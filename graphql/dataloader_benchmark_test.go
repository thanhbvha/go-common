package graphql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkDataLoader(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("keys_%d", n), func(b *testing.B) {
			ctx, cancel := context.WithTimeout(b.Context(), 2*time.Minute)
			defer cancel()
			var calls, fetched atomic.Int64
			keys := make([]int, n)
			for i := range keys {
				keys[i] = i
			}
			b.ReportAllocs()
			for b.Loop() {
				loader, err := NewDataLoader(ctx, func(_ context.Context, keys []int) (map[int]int, error) {
					calls.Add(1)
					fetched.Add(int64(len(keys)))
					out := make(map[int]int, len(keys))
					for _, k := range keys {
						out[k] = k
					}
					return out, nil
				}, DefaultLoaderConfig())
				if err != nil {
					b.Fatal(err)
				}
				values, errs := loader.LoadAll(ctx, keys)
				for i, key := range keys {
					if errs[i] != nil || values[i] != key {
						b.Fatal("incorrect loader result")
					}
				}
			}
			if fetched.Load() != int64(b.N*n) || calls.Load() < int64(b.N*((n+99)/100)) || calls.Load() > int64(b.N*n) {
				b.Fatalf("unexpected backend calls: %d", calls.Load())
			}
			b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
		})
	}
}

// BenchmarkDataLoaderCached isolates the hot path from batching latency.
func BenchmarkDataLoaderCached(b *testing.B) {
	ctx, cancel := context.WithTimeout(b.Context(), 2*time.Minute)
	defer cancel()
	l, err := NewDataLoader(ctx, func(context.Context, []int) (map[int]int, error) { return nil, nil }, DefaultLoaderConfig())
	if err != nil {
		b.Fatal(err)
	}
	if !l.Prime(1, 1) {
		b.Fatal("prime failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if v, err := l.Load(ctx, 1); err != nil || v != 1 {
			b.Fatal(v, err)
		}
	}
}
func BenchmarkDataLoaderDuplicateKeys(b *testing.B) {
	ctx, cancel := context.WithTimeout(b.Context(), 2*time.Minute)
	defer cancel()
	keys := make([]int, 1000)
	for i := range keys {
		keys[i] = i % 10
	}
	var calls, fetched atomic.Int64
	b.ReportAllocs()
	for b.Loop() {
		l, err := NewDataLoader(ctx, func(_ context.Context, keys []int) (map[int]int, error) {
			calls.Add(1)
			fetched.Add(int64(len(keys)))
			out := make(map[int]int, len(keys))
			for _, k := range keys {
				out[k] = k
			}
			return out, nil
		}, DefaultLoaderConfig())
		if err != nil {
			b.Fatal(err)
		}
		values, errs := l.LoadAll(ctx, keys)
		for i, key := range keys {
			if errs[i] != nil || values[i] != key {
				b.Fatal("incorrect duplicate-key result")
			}
		}
	}
	b.StopTimer()
	if fetched.Load() != int64(b.N*10) || calls.Load() < int64(b.N) || calls.Load() > int64(b.N*10) {
		b.Fatalf("unexpected backend calls: %d", calls.Load())
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
}
func BenchmarkDataLoaderParallelRequests(b *testing.B) {
	ctx, cancel := context.WithTimeout(b.Context(), 2*time.Minute)
	defer cancel()
	var calls, fetched atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := NewDataLoader(ctx, func(_ context.Context, keys []int) (map[int]int, error) {
				calls.Add(1)
				fetched.Add(int64(len(keys)))
				out := make(map[int]int, len(keys))
				for _, k := range keys {
					out[k] = k
				}
				return out, nil
			}, DefaultLoaderConfig())
			if err != nil {
				b.Error(err)
				return
			}
			values, errs := l.LoadAll(ctx, []int{1, 2, 3, 1})
			if len(values) != 4 || values[0] != 1 || values[1] != 2 || values[2] != 3 || values[3] != 1 {
				b.Error("incorrect parallel loader result")
				return
			}
			for _, err := range errs {
				if err != nil {
					b.Error(err)
					return
				}
			}
		}
	})
	b.StopTimer()
	if fetched.Load() != int64(b.N*3) || calls.Load() < int64(b.N) || calls.Load() > int64(b.N*3) {
		b.Fatalf("unexpected backend calls: %d", calls.Load())
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
}
