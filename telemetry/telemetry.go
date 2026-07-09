// Package telemetry provides production-ready OpenTelemetry integration.
//
// It simplifies the configuration and initialization of OTLP (OpenTelemetry Protocol)
// exporters for both distributed tracing and metrics gathering. It provides helper
// functions to start spans, record metrics, and attach attributes.
//
// Basic usage:
//
//	tel, err := telemetry.Init(ctx, telemetry.Config{
//		ServiceName:   "my-service",
//		EnableTracing: true,
//		EnableMetrics: true,
//		Endpoint:      "localhost:4317",
//	})
//	defer tel.Shutdown(ctx)
//
//	ctx, span := telemetry.StartSpan(ctx, "HandleRequest")
//	defer span.End()
package telemetry

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Telemetry holds the shutdown functions.
type Telemetry struct {
	shutdownFuncs []func(context.Context) error
}

// Init initializes OpenTelemetry based on the provided configuration.
func Init(ctx context.Context, cfg Config) (*Telemetry, error) {
	t := &Telemetry{}

	// Create resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentKey.String(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if cfg.EnableTracing {
		traceProvider, err := initTracerProvider(ctx, res, cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		otel.SetTracerProvider(traceProvider)
		t.shutdownFuncs = append(t.shutdownFuncs, traceProvider.Shutdown)
		log.Printf("[Telemetry] Tracing enabled for service '%s', exporting to %s", cfg.ServiceName, cfg.Endpoint)
	}

	if cfg.EnableMetrics {
		meterProvider, err := initMeterProvider(ctx, res, cfg.Endpoint)
		if err != nil {
			return nil, err
		}
		otel.SetMeterProvider(meterProvider)
		t.shutdownFuncs = append(t.shutdownFuncs, meterProvider.Shutdown)
		log.Printf("[Telemetry] Metrics enabled for service '%s', exporting to %s", cfg.ServiceName, cfg.Endpoint)
	}

	return t, nil
}

// Shutdown gracefully shuts down all OTel providers.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range t.shutdownFuncs {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to shutdown telemetry: %v", errs)
	}
	return nil
}

func initTracerProvider(ctx context.Context, res *resource.Resource, endpoint string) (*trace.TracerProvider, error) {
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // Adjust for production if using TLS
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	bsp := trace.NewBatchSpanProcessor(traceExporter)
	traceProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()), // For production, you might want trace.ParentBased(trace.TraceIDRatioBased(0.1))
		trace.WithResource(res),
		trace.WithSpanProcessor(bsp),
	)

	return traceProvider, nil
}

func initMeterProvider(ctx context.Context, res *resource.Resource, endpoint string) (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(), // Adjust for production if using TLS
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 15s for demonstration purposes.
			metric.WithInterval(15*time.Second),
		)),
	)

	return meterProvider, nil
}
