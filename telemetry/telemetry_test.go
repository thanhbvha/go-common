package telemetry

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"sync/atomic"
	"testing"
)

type trackingExporter struct{ stopped atomic.Int32 }

func (e *trackingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (e *trackingExporter) Shutdown(context.Context) error                             { e.stopped.Add(1); return nil }
func TestPartialInitializationRollsBackWithoutChangingGlobals(t *testing.T) {
	oldT, oldM, oldP := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	exporter := &trackingExporter{}
	sentinel := errors.New("metric failure")
	cfg := DefaultConfig("test")
	_, err := newTelemetry(t.Context(), cfg, func(context.Context, *resource.Resource, Config) (*sdktrace.TracerProvider, error) {
		return sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)), nil
	}, func(context.Context, *resource.Resource, Config) (*sdkmetric.MeterProvider, error) {
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) || exporter.stopped.Load() != 1 {
		t.Fatalf("err=%v stopped=%d", err, exporter.stopped.Load())
	}
	if oldT != otel.GetTracerProvider() || oldM != otel.GetMeterProvider() || oldP != otel.GetTextMapPropagator() {
		t.Fatal("globals changed during failed construction")
	}
}
func TestZeroSamplingAndExplicitGlobalInstallation(t *testing.T) {
	old := otel.GetTracerProvider()
	oldMeter, oldPropagator := otel.GetMeterProvider(), otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(old)
		otel.SetMeterProvider(oldMeter)
		otel.SetTextMapPropagator(oldPropagator)
	}()
	cfg := DefaultConfig("test")
	cfg.EnableMetrics = false
	cfg.SamplingRate = 0
	cfg.Insecure = true
	runtime, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if old != otel.GetTracerProvider() {
		t.Fatal("constructor installed globals")
	}
	_, span := runtime.TracerProvider().Tracer("test").Start(t.Context(), "unsampled", trace.WithAttributes(attribute.Bool("test", true)))
	if span.IsRecording() {
		t.Fatal("sampling zero recorded root span")
	}
	span.End()
	if err := runtime.InstallGlobal(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.InstallGlobal(); err == nil {
		t.Fatal("installed closed runtime")
	}
}
func TestInvalidConfigAndCanceledStartup(t *testing.T) {
	for _, rate := range []float64{-1, 1.1} {
		cfg := DefaultConfig("test")
		cfg.SamplingRate = rate
		if _, err := New(t.Context(), cfg); err == nil {
			t.Fatal("invalid sampling accepted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := New(ctx, DefaultConfig("test")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
