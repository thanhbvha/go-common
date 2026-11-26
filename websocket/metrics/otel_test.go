package metrics

import (
	"context"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/core"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRegisterExportsManagerMetrics(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	unregister, err := Register(meter, Sources{Manager: manager})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unregister() }()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			seen[m.Name] = true
			if m.Name == "websocket.ready" {
				if v := m.Data.(metricdata.Gauge[int64]).DataPoints[0].Value; v != 1 {
					t.Fatalf("ready = %d, want 1", v)
				}
			}
		}
	}
	for _, name := range []string{"websocket.connections", "websocket.ready", "websocket.slow_consumers", "websocket.drops"} {
		if !seen[name] {
			t.Errorf("metric %s not exported", name)
		}
	}
	if seen["websocket.gateway.sessions"] {
		t.Error("gateway metrics exported without a gateway")
	}
}

func TestRegisterRequiresManager(t *testing.T) {
	meter := sdkmetric.NewMeterProvider().Meter("test")
	if _, err := Register(meter, Sources{}); err == nil {
		t.Fatal("want error without a manager")
	}
}
