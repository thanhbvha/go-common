package graphql

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsBoundedLabelsAndBalancedInflight(t *testing.T) {
	old := otel.GetMeterProvider()
	defer otel.SetMeterProvider(old)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	otel.SetMeterProvider(provider)
	s := newTestServer(t, Config{EnableTelemetry: true, MaxDepth: 1}, nil)
	query(s, "query secretClientName {value}")
	request(s, `{"secret"`)
	query(s, "{node{value}}")
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	sums := map[string]int64{}
	var durationCount uint64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch v := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range v.DataPoints {
					for _, a := range point.Attributes.ToSlice() {
						if string(a.Key) != "graphql.operation.type" {
							t.Fatalf("unexpected metric label %v", a)
						}
						kind := a.Value.AsString()
						if kind != "query" && kind != "mutation" && kind != "unknown" {
							t.Fatal(kind)
						}
					}
					sums[m.Name] += point.Value
				}
			case metricdata.Histogram[float64]:
				for _, point := range v.DataPoints {
					durationCount += point.Count
				}
			}
		}
	}
	if sums["graphql.request.inflight"] != 0 || sums["graphql.request.errors"] != 2 || sums["graphql.request.rejected"] != 2 || durationCount != 3 {
		t.Fatal(sums, durationCount)
	}
}
