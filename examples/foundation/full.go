package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/foundation"
	foundationhttp "github.com/thanhbvha/go-common/foundation/adapter/nethttp"
	"github.com/thanhbvha/go-common/health"
	healthhttp "github.com/thanhbvha/go-common/health/handler/nethttp"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/telemetry"
)

func runFull(ctx context.Context) error {
	serviceLogger := logger.New(logger.DefaultOptions())
	logger.SetDefault(serviceLogger)
	defer serviceLogger.Close()

	app, err := foundation.New(
		foundation.Config{Name: "foundation-full", Version: "v1.0.0"},
		foundation.WithLogger(serviceLogger),
	)
	if err != nil {
		return err
	}

	var observability *telemetry.Telemetry
	if err := app.Register(foundation.Resource("telemetry", func(ctx context.Context) error {
		cfg := telemetry.DefaultConfig("foundation-full")
		cfg.ServiceVersion = "v1.0.0"
		cfg.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		cfg.EnableTracing = cfg.Endpoint != ""
		cfg.EnableMetrics = cfg.Endpoint != ""
		var initErr error
		observability, initErr = telemetry.Init(ctx, cfg)
		return initErr
	}, func(ctx context.Context) error {
		if observability == nil {
			return nil
		}
		return observability.Shutdown(ctx)
	})); err != nil {
		return err
	}

	dependency := &demoDependency{}
	app.Health().AddReadinessCheck("demo-database", health.CustomChecker(dependency.Check))
	if err := app.Register(foundation.Resource("demo-database", dependency.Connect, dependency.Close)); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /health", healthhttp.Handler(app.Health()))
	mux.Handle("GET /ready", healthhttp.ReadyHandler(app.Health()))
	mux.Handle("GET /live", healthhttp.LiveHandler(app.Health()))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "full foundation service is ready")
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := app.Register(foundationhttp.Component("http", server)); err != nil {
		return err
	}
	return app.Run(ctx)
}

type demoDependency struct{ connected atomic.Bool }

func (d *demoDependency) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.connected.Store(true)
	return nil
}

func (d *demoDependency) Close(context.Context) error {
	d.connected.Store(false)
	return nil
}

func (d *demoDependency) Check(context.Context) error {
	if !d.connected.Load() {
		return errors.New("demo database is disconnected")
	}
	return nil
}
