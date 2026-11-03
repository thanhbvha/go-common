// Package telemetry builds explicitly owned OpenTelemetry providers. Global
// installation is an opt-in bootstrap operation after successful construction.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	apimetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	apitrace "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/credentials"
	"maps"
	"sync"
	"time"
)

type Telemetry struct {
	tracer        apitrace.TracerProvider
	meter         apimetric.MeterProvider
	propagator    propagation.TextMapPropagator
	shutdownFuncs []func(context.Context) error
	mu            sync.Mutex
	stopping      bool
	done          chan struct{}
	shutdownErr   error
}
type traceFactory func(context.Context, *resource.Resource, Config) (*trace.TracerProvider, error)
type meterFactory func(context.Context, *resource.Resource, Config) (*metric.MeterProvider, error)

func New(ctx context.Context, cfg Config) (*Telemetry, error) {
	return newTelemetry(ctx, cfg, initTracerProvider, initMeterProvider)
}
func newTelemetry(ctx context.Context, cfg Config, tf traceFactory, mf meterFactory) (*Telemetry, error) {
	cfg.Headers = maps.Clone(cfg.Headers)
	if cfg.TLSConfig != nil {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(cfg.ServiceName), semconv.ServiceVersionKey.String(cfg.ServiceVersion), semconv.DeploymentEnvironmentKey.String(cfg.Environment)))
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}
	propagators := []propagation.TextMapPropagator{propagation.TraceContext{}}
	if cfg.EnableBaggage {
		propagators = append(propagators, propagation.Baggage{})
	}
	t := &Telemetry{tracer: tracenoop.NewTracerProvider(), meter: metricnoop.NewMeterProvider(), propagator: propagation.NewCompositeTextMapPropagator(propagators...), done: make(chan struct{})}
	rollback := func(cause error) (*Telemetry, error) {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return nil, errors.Join(cause, t.Shutdown(cleanup))
	}
	if cfg.EnableTracing {
		p, err := tf(ctx, res, cfg)
		if err != nil {
			return rollback(err)
		}
		t.tracer = p
		t.shutdownFuncs = append(t.shutdownFuncs, p.Shutdown)
	}
	if cfg.EnableMetrics {
		p, err := mf(ctx, res, cfg)
		if err != nil {
			return rollback(err)
		}
		t.meter = p
		t.shutdownFuncs = append(t.shutdownFuncs, p.Shutdown)
	}
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	return t, nil
}
func (t *Telemetry) TracerProvider() apitrace.TracerProvider   { return t.tracer }
func (t *Telemetry) MeterProvider() apimetric.MeterProvider    { return t.meter }
func (t *Telemetry) Propagator() propagation.TextMapPropagator { return t.propagator }

// InstallGlobal is intended for a single process bootstrap owner. It must run
// before concurrent serving; shutdown does not restore or replace globals.
func (t *Telemetry) InstallGlobal() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return errors.New("telemetry: runtime closed")
	}
	otel.SetTracerProvider(t.tracer)
	otel.SetMeterProvider(t.meter)
	otel.SetTextMapPropagator(t.propagator)
	return nil
}

// Shutdown bounds the caller's wait; cleanup continues once, in reverse order.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if !t.stopping {
		t.stopping = true
		go func() {
			var errs []error
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			for i := len(t.shutdownFuncs) - 1; i >= 0; i-- {
				errs = append(errs, t.shutdownFuncs[i](cleanup))
			}
			t.shutdownErr = errors.Join(errs...)
			close(t.done)
		}()
	}
	t.mu.Unlock()
	select {
	case <-t.done:
		return t.shutdownErr
	default:
	}
	select {
	case <-t.done:
		return t.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func initTracerProvider(ctx context.Context, res *resource.Resource, cfg Config) (*trace.TracerProvider, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.TLSConfig != nil {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(cfg.TLSConfig.Clone())))
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	samplingRate := cfg.SamplingRate

	sampler := trace.ParentBased(trace.TraceIDRatioBased(samplingRate))

	bsp := trace.NewBatchSpanProcessor(traceExporter)
	traceProvider := trace.NewTracerProvider(
		trace.WithSampler(sampler),
		trace.WithResource(res),
		trace.WithSpanProcessor(bsp),
	)

	return traceProvider, nil
}

func initMeterProvider(ctx context.Context, res *resource.Resource, cfg Config) (*metric.MeterProvider, error) {
	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.TLSConfig != nil {
		opts = append(opts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(cfg.TLSConfig.Clone())))
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
	}
	if cfg.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	interval := cfg.ExportInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(interval),
		)),
	)

	return meterProvider, nil
}
