package health_test

import (
	"context"
	"github.com/thanhbvha/go-common/v2/health"
	"testing"
)

func BenchmarkReadiness(b *testing.B) {
	checker := health.New().AddReadinessCheck("dependency", func(context.Context) error { return nil })
	b.ReportAllocs()
	for b.Loop() {
		_ = checker.CheckReadiness(b.Context())
	}
}
