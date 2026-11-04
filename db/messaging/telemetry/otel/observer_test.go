package otel

import (
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsHaveOnlyBoundedLabels(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	o, err := New(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	o.Observe(t.Context(), messaging.Observation{Operation: "publish", Outcome: "retry", Duration: time.Second})
	o.Observe(t.Context(), messaging.Observation{Operation: "payload-secret", Outcome: "private"})
	o.Observe(t.Context(), messaging.Observation{Stats: &messaging.Stats{Pending: 2, Dead: 1, OldestPendingAge: time.Second}})
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			count++
			if m.Name == "messaging.operations" {
				sum := m.Data.(metricdata.Sum[int64])
				if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 || sum.DataPoints[0].Attributes.Len() != 2 {
					t.Fatal("unbounded labels", sum)
				}
			}
		}
	}
	if count != 6 {
		t.Fatal("missing metrics", count)
	}
}
