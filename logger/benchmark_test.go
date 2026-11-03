package logger

import (
	"io"
	"testing"
)

func BenchmarkLogging(b *testing.B) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		b.Run(name, func(b *testing.B) {
			l := New(Options{Writer: io.Discard, Async: async, BufferSize: 1024})
			b.Cleanup(l.Close)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.Info("request completed", "route", "/v1/me", "status", 200)
				}
			})
		})
	}
}
