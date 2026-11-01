package graphql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkDataLoader(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("keys_%d", n), func(b *testing.B) {
			var calls atomic.Int64
			keys := make([]int, n)
			for i := range keys {
				keys[i] = i
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				loader, err := NewDataLoader(context.Background(), func(_ context.Context, keys []int) (map[int]int, error) {
					calls.Add(1)
					out := make(map[int]int, len(keys))
					for _, k := range keys {
						out[k] = k
					}
					return out, nil
				}, LoaderConfig{MaxBatch: 100})
				if err != nil {
					b.Fatal(err)
				}
				_, errs := loader.LoadAll(context.Background(), keys)
				for _, err := range errs {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
		})
	}
}

// BenchmarkDataLoaderCached isolates the hot path from batching latency.
func BenchmarkDataLoaderCached(b *testing.B) {
	l, err := NewDataLoader(context.Background(), func(context.Context, []int) (map[int]int, error) { return nil, nil }, LoaderConfig{})
	if err != nil {
		b.Fatal(err)
	}
	l.Prime(1, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v, err := l.Load(context.Background(), 1); err != nil || v != 1 {
			b.Fatal(v, err)
		}
	}
}
func BenchmarkDataLoaderDuplicateKeys(b *testing.B) {
	keys := make([]int, 1000)
	for i := range keys {
		keys[i] = i % 10
	}
	var calls atomic.Int64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l, err := NewDataLoader(context.Background(), func(_ context.Context, keys []int) (map[int]int, error) {
			calls.Add(1)
			out := make(map[int]int, len(keys))
			for _, k := range keys {
				out[k] = k
			}
			return out, nil
		}, LoaderConfig{})
		if err != nil {
			b.Fatal(err)
		}
		_, errs := l.LoadAll(context.Background(), keys)
		for _, err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
}
func BenchmarkDataLoaderParallelRequests(b *testing.B) {
	var calls atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l, err := NewDataLoader(context.Background(), func(_ context.Context, keys []int) (map[int]int, error) {
				calls.Add(1)
				out := make(map[int]int, len(keys))
				for _, k := range keys {
					out[k] = k
				}
				return out, nil
			}, LoaderConfig{})
			if err != nil {
				b.Error(err)
				return
			}
			_, errs := l.LoadAll(context.Background(), []int{1, 2, 3, 1})
			for _, err := range errs {
				if err != nil {
					b.Error(err)
					return
				}
			}
		}
	})
	b.ReportMetric(float64(calls.Load())/float64(b.N), "backend_calls/op")
}
