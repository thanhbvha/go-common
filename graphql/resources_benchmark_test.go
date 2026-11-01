package graphql

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// This separate benchmark samples goroutines inside active backend batches.
// It deliberately adds backend latency and is not comparable to the hot-path benchmarks.
func BenchmarkDataLoaderGoroutines(b *testing.B) {
	keys := make([]int, 1000)
	for i := range keys {
		keys[i] = i
	}
	baseline := int64(runtime.NumGoroutine())
	var peak atomic.Int64
	peak.Store(baseline)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l, err := NewDataLoader(context.Background(), func(_ context.Context, ks []int) (map[int]int, error) {
			n := int64(runtime.NumGoroutine())
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			out := make(map[int]int, len(ks))
			for _, k := range ks {
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
	b.ReportMetric(float64(peak.Load()-baseline), "extra_goroutines_peak")
}
