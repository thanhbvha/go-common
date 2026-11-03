package grpc_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/foundation"
	"github.com/thanhbvha/go-common/v2/foundation/adapter/grpc"
	common "github.com/thanhbvha/go-common/v2/grpc"
	grpcframework "google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestComponentStartsAndStops(t *testing.T) {
	address := availableAddress(t)
	server := grpcframework.NewServer()
	component := grpc.Component("grpc", address, server)
	ctx, cancel := context.WithCancel(t.Context())
	if err := component.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- component.Run(ctx) }()

	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := component.Stop(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != context.Canceled {
		t.Fatalf("expected canceled run, got %v", err)
	}
}

func TestStartupRollbackReleasesListener(t *testing.T) {
	address := availableAddress(t)
	app, err := foundation.New(foundation.Config{Name: "rollback"}, foundation.WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	server := grpcframework.NewServer()
	if err := app.Register(grpc.Component("grpc", address, server)); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(foundation.Resource("failure", func(context.Context) error {
		return errors.New("startup failed")
	}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(t.Context()); err == nil {
		t.Fatal("expected startup failure")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listener leaked after rollback: %v", err)
	}
	_ = listener.Close()
}

func TestComponentReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := grpcframework.NewServer()
	if err := grpc.Component("grpc", listener.Addr().String(), server).Start(t.Context()); err == nil {
		t.Fatal("expected bind failure")
	}
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func TestWrapperShutdownCallback(t *testing.T) {
	address := availableAddress(t)
	server := common.NewServer(common.ServerConfig{EnableHealth: true, ShutdownTimeout: time.Second})
	terminal := errors.New("application shutdown failure")
	called := false
	component := grpc.ComponentWithShutdown("grpc", address, server.Server, func(ctx context.Context) error {
		called = true
		return errors.Join(server.Shutdown(ctx), terminal)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := component.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- component.Run(ctx) }()
	cancel()
	stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := component.Stop(stopCtx); !errors.Is(err, terminal) {
		t.Fatalf("callback failure lost: %v", err)
	}
	if !called {
		t.Fatal("shutdown callback not used")
	}
	resp, err := server.Health.Check(stopCtx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("wrapper health not shut down: %v %v", resp, err)
	}
	select {
	case <-done:
	case <-stopCtx.Done():
		t.Fatal("runner did not stop")
	}
}
