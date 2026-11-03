package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/foundation"
	foundationhttp "github.com/thanhbvha/go-common/v2/foundation/adapter/nethttp"
	healthhttp "github.com/thanhbvha/go-common/v2/health/handler/nethttp"
)

var errDemoRuntime = errors.New("demo worker failed")

func runLifecycle(ctx context.Context, scenario, address string, stopAfter time.Duration) error {
	if stopAfter < 0 {
		return errors.New("stop-after must not be negative")
	}
	app, _, err := newLifecycleApp(scenario, address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if stopAfter > 0 {
		timer := time.AfterFunc(stopAfter, cancel)
		defer timer.Stop()
	}
	return app.Run(ctx)
}

// All dependencies are in memory: no database or broker is required.
func newLifecycleApp(scenario, address string) (*foundation.App, *http.Server, error) {
	switch scenario {
	case "normal", "startup-timeout", "stop-timeout", "runner-timeout", "runtime-failure", "missing", "cycle":
	default:
		return nil, nil, fmt.Errorf("unknown lifecycle scenario %q", scenario)
	}
	app, err := foundation.New(foundation.Config{Name: "foundation-lifecycle", StartupTimeout: time.Second, ShutdownTimeout: time.Second})
	if err != nil {
		return nil, nil, err
	}
	database := &demoDependency{}
	var processed atomic.Uint64
	app.Health().AddReadinessCheck("database", database.Check)
	mux := http.NewServeMux()
	mux.Handle("GET /ready", healthhttp.ReadyHandler(app.Health()))
	mux.Handle("GET /live", healthhttp.LiveHandler(app.Health()))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "processed=%d\n", processed.Load())
	})
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	httpComponent := foundationhttp.Component("http", server)
	httpComponent.DependsOn = []string{"worker"}
	worker := foundation.Component{
		Name: "worker", DependsOn: []string{"database"},
		Start: func(ctx context.Context) error {
			slog.Info("start worker")
			if err := database.Check(ctx); err != nil {
				return err
			}
			if scenario == "startup-timeout" {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
		Run: func(ctx context.Context) error {
			if scenario == "runtime-failure" {
				return errDemoRuntime
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					// Intentionally exceed the shutdown budget, but eventually return.
					if scenario == "runner-timeout" {
						time.Sleep(1500 * time.Millisecond)
					}
					slog.Info("worker drained", "processed", processed.Load())
					return ctx.Err()
				case <-ticker.C:
					if err := database.Check(ctx); err != nil {
						return err
					}
					processed.Add(1)
				}
			}
		},
		Stop: func(ctx context.Context) error {
			slog.Info("stop worker")
			if scenario == "stop-timeout" {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}
	db := foundation.Resource("database", func(ctx context.Context) error {
		slog.Info("start database")
		return database.Connect(ctx)
	}, func(ctx context.Context) error {
		slog.Info("stop database")
		return database.Close(ctx)
	})
	if scenario == "missing" {
		worker.DependsOn = []string{"unregistered"}
	}
	if scenario == "cycle" {
		db.DependsOn = []string{"http"}
	}
	// Deliberately register dependents first. Foundation resolves the graph before
	// startup: database -> worker -> http; shutdown reverses that order.
	for _, c := range []foundation.Component{httpComponent, worker, db} {
		if err := app.Register(c); err != nil {
			return nil, nil, err
		}
	}
	return app, server, nil
}
