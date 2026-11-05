package logger

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type benchmarkWriter struct{ records atomic.Int64 }

func (w *benchmarkWriter) Write(p []byte) (int, error) { w.records.Add(1); return len(p), nil }

func BenchmarkLogging(b *testing.B) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		b.Run(name, func(b *testing.B) {
			writer := &benchmarkWriter{}
			var failures atomic.Int64
			l := New(Options{Writer: writer, Async: async, BufferSize: 1024, OnError: func(error) { failures.Add(1) }})
			b.Cleanup(l.Close)
			// Initialize encoding buffers before measuring admission. Async uses
			// OverflowSync, so a full queue preserves records on the caller.
			l.Info("warmup")
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.Info("request completed", "route", "/v1/me", "status", 200)
				}
			})
			b.StopTimer()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := l.Shutdown(ctx); err != nil {
				b.Fatal(err)
			}
			if writer.records.Load() != int64(b.N)+1 || failures.Load() != 0 {
				b.Fatalf("records=%d failures=%d", writer.records.Load(), failures.Load())
			}
			b.ReportMetric(1, "written_records/op")
		})
	}
}
