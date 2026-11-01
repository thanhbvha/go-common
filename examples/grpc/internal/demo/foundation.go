package demo

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thanhbvha/go-common/foundation"
	grpcadapter "github.com/thanhbvha/go-common/foundation/adapter/grpc"
	common "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// exampleDependency represents a database/broker initialized before serving.
// Replace its Start/Stop implementation with a real connection lifecycle.
type exampleDependency struct{ ready atomic.Bool }

// RunFoundation uses the same services, auth, TLS and configuration as RunServer.
// Only foundation owns signals and component lifecycle; no additional serve loop
// or global graceful callbacks are started by this command.
func RunFoundation(ctx context.Context, o ServerOptions) (result error) {
	token, err := sharedToken(o.Token)
	if err != nil {
		return err
	}
	srv, err := newServer(o, token)
	if err != nil {
		return err
	}
	defer combineCleanup(&result, func() error { return srv.Shutdown(context.Background()) })
	dependency := &exampleDependency{}
	app, err := newFoundationApp(o, srv, dependency)
	if err != nil {
		return err
	}
	if err = app.Run(ctx); err != nil {
		// Keep terminal error classification without exposing transport/config values.
		return foundationFailure(err)
	}
	return nil
}

func newFoundationApp(o ServerOptions, srv *common.Server, dependency *exampleDependency, options ...foundation.Option) (*foundation.App, error) {
	if o.StartupTimeout <= 0 || o.ShutdownTimeout <= 0 || o.AppShutdownTimeout <= o.ShutdownTimeout {
		return nil, errors.New("foundation requires positive startup/drain budgets and app-shutdown-timeout greater than shutdown-timeout")
	}
	app, err := foundation.New(foundation.Config{
		Name: "grpc-example", StartupTimeout: o.StartupTimeout, ShutdownTimeout: o.AppShutdownTimeout,
	}, options...)
	if err != nil {
		return nil, errors.New("foundation configuration failed")
	}
	// Start: telemetry -> dependency -> gRPC. Stop: gRPC -> dependency -> telemetry.
	var flush func(context.Context) error
	if err = app.Register(foundation.Resource("telemetry", func(ctx context.Context) error {
		var err error
		flush, err = initTelemetryProvider(ctx, "grpc-example-foundation", o.CommonOptions)
		return err
	}, func(ctx context.Context) error {
		if flush == nil {
			return nil
		}
		if err := flush(ctx); err != nil {
			return errors.New("telemetry flush failed")
		}
		logger.Info("Foundation telemetry stopped")
		return nil
	})); err != nil {
		return nil, err
	}
	if err = app.Register(foundation.Resource("dependency", func(ctx context.Context) error {
		logger.Info("Foundation dependency initializing")
		if err := pause(ctx, o.ReadyDelay); err != nil {
			return err
		}
		dependency.ready.Store(true)
		logger.Info("Foundation dependency ready")
		return nil
	}, func(context.Context) error {
		dependency.ready.Store(false)
		logger.Info("Foundation dependency closed after gRPC runner stopped")
		return nil
	})); err != nil {
		return nil, err
	}
	component := grpcadapter.ComponentWithShutdown("grpc", o.Address, srv.Server, func(ctx context.Context) error {
		logger.Info("Foundation gRPC drain starting")
		err := srv.Shutdown(ctx)
		logger.Info("Foundation gRPC drain finished", "forced", err != nil)
		return err
	})
	bind := component.Start
	component.Start = func(ctx context.Context) error {
		if !dependency.ready.Load() {
			return errors.New("gRPC dependency is not ready")
		}
		if err := bind(ctx); err != nil {
			return errors.New("gRPC listener startup failed")
		}
		srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		srv.Health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
		logger.Info("Foundation gRPC ready", "address", o.Address, "tls", !o.Insecure)
		return nil
	}
	if err = app.Register(component); err != nil {
		return nil, err
	}
	return app, nil
}

func foundationFailure(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(errors.New("foundation lifecycle deadline exceeded"), context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return errors.Join(errors.New("foundation lifecycle canceled during startup/shutdown"), context.Canceled)
	}
	return errors.New("foundation lifecycle failed")
}
