package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/thanhbvha/go-common/v2/db/orm"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Local SDK providers keep the tutorial runnable without an exporter/collector.
// A service can supply OTLP-configured providers through the same Config fields.
// No process-global SetTracerProvider/SetMeterProvider is used.
type localTelemetry struct {
	traces  *sdktrace.TracerProvider
	metrics *sdkmetric.MeterProvider
	reader  *sdkmetric.ManualReader
	spans   *tracetest.SpanRecorder
	events  atomic.Uint64
}

func newLocalTelemetry() *localTelemetry {
	recorder := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	return &localTelemetry{traces: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)), metrics: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), reader: reader, spans: recorder}
}
func (t *localTelemetry) observe(event orm.Event) {
	// Monitor is synchronous: do bounded work only, remain concurrency-safe, and
	// never call manager/client lifecycle methods from this callback.
	switch event.Kind {
	case orm.EventAdmission, orm.EventTransaction:
		t.events.Add(1)
	}
}
func (t *localTelemetry) close() error {
	ctx, cancel := cleanupContext()
	defer cancel()
	return errors.Join(nativeFailure("metrics shutdown", t.metrics.Shutdown(ctx)), nativeFailure("trace shutdown", t.traces.Shutdown(ctx)))
}
func demoTelemetry(ctx context.Context, opt options) error {
	opt.otel = true
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		const marker = "synthetic-sensitive-marker"
		row := patient("telemetry", "one")
		row.Name = marker
		if err := f.client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			return bound.Create(tx.Context(), &row)
		}); err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, row.ID); err != nil {
			return err
		}
		if err := f.client.Ping(ctx); err != nil {
			return err
		}
		var snapshot metricdata.ResourceMetrics
		if err := f.telemetry.reader.Collect(ctx, &snapshot); err != nil {
			return nativeFailure("metric collection", err)
		}
		instruments := 0
		for _, scope := range snapshot.ScopeMetrics {
			instruments += len(scope.Metrics)
		}
		spans := f.telemetry.spans.Ended()
		for _, span := range spans {
			if strings.Contains(fmt.Sprint(span.Name(), span.Attributes(), span.Events(), span.Status()), marker) {
				return fmt.Errorf("sensitive value appeared in span metadata")
			}
		}
		pool, admission, transactions := f.client.Stats(), f.client.AdmissionStats(), f.client.TransactionStats()
		fmt.Printf("  spans=%d instruments=%d monitor_events=%d\n", len(spans), instruments, f.telemetry.events.Load())
		fmt.Printf("  pool open=%d in_use=%d idle=%d waits=%d wait_time=%s\n", pool.OpenConnections, pool.InUse, pool.Idle, pool.WaitCount, pool.WaitDuration)
		fmt.Printf("  admission admitted=%d queued=%d rejected=%d; transactions started=%d committed=%d failed=%d unknown=%d\n", admission.Admitted, admission.Queued, admission.Rejected, transactions.Started, transactions.Committed, transactions.Failed, transactions.Unknown)
		fmt.Println("  span payload privacy verified; client closes before SDK provider shutdown")
		return nil
	})
}
