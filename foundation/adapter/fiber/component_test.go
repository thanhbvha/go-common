package fiber_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/foundation"
	"github.com/thanhbvha/go-common/foundation/adapter/fiber"
)

func TestComponentServesAndStops(t *testing.T) {
	address := availableAddress(t)
	app := fiberframework.New(fiberframework.Config{DisableStartupMessage: true, ReadTimeout: time.Second})
	app.Get("/", func(c *fiberframework.Ctx) error { return c.SendString("ok") })
	component := fiber.Component("fiber", address, app)
	ctx, cancel := context.WithCancel(t.Context())
	if err := component.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- component.Run(ctx) }()

	var response *http.Response
	var err error
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err = http.Get("http://" + address)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "ok" {
		t.Fatalf("body=%q err=%v", body, err)
	}

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

func TestComponentReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	app := fiberframework.New(fiberframework.Config{DisableStartupMessage: true, ReadTimeout: time.Second})
	if err := fiber.Component("fiber", listener.Addr().String(), app).Start(t.Context()); err == nil {
		t.Fatal("expected bind failure")
	}
}

func TestComponentRejectsMissingReadTimeout(t *testing.T) {
	app := fiberframework.New(fiberframework.Config{DisableStartupMessage: true})
	if err := fiber.Component("fiber", "127.0.0.1:0", app).Start(t.Context()); err == nil {
		t.Fatal("expected strict timeout validation failure")
	}
	component := fiber.Component("fiber", "127.0.0.1:0", app, fiber.AllowUnsafeTimeouts())
	if err := component.Start(t.Context()); err != nil {
		t.Fatalf("explicit unsafe timeout opt-out failed: %v", err)
	}
	if err := component.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupRollbackReleasesListener(t *testing.T) {
	address := availableAddress(t)
	app, err := foundation.New(foundation.Config{Name: "rollback"}, foundation.WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	fiberApp := fiberframework.New(fiberframework.Config{DisableStartupMessage: true, ReadTimeout: time.Second})
	if err := app.Register(fiber.Component("fiber", address, fiberApp)); err != nil {
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
