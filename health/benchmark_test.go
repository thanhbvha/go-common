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
		if result := checker.CheckReadiness(b.Context()); result.Status != health.StatusOK || len(result.Checks) != 1 {
			b.Fatal("readiness did not complete successfully")
		}
	}
}
