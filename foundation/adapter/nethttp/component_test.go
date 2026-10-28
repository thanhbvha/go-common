package nethttp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/foundation"
	"github.com/thanhbvha/go-common/foundation/adapter/nethttp"
)

func TestComponentServesAndStops(t *testing.T) {
	server := &http.Server{
		Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "ok")
		}),
		ReadHeaderTimeout: time.Second,
	}
	component := nethttp.Component("http", server)
	ctx, cancel := context.WithCancel(t.Context())
	if err := component.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- component.Run(ctx) }()

	response, err := http.Get("http://" + server.Addr)
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
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}
	if err := nethttp.Component("http", server).Start(t.Context()); err == nil {
		t.Fatal("expected bind failure")
	}
}

func TestComponentRejectsUnsafeServerConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		server *http.Server
	}{
		{name: "nil handler", server: &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}},
		{name: "missing timeout", server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := nethttp.Component("http", test.server).Start(t.Context()); err == nil {
				t.Fatal("expected strict validation failure")
			}
		})
	}

	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	component := nethttp.Component("http", server, nethttp.AllowUnsafeTimeouts())
	if err := component.Start(t.Context()); err != nil {
		t.Fatalf("explicit unsafe timeout opt-out failed: %v", err)
	}
	if err := component.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupRollbackReleasesListener(t *testing.T) {
	app, err := foundation.New(foundation.Config{Name: "rollback"}, foundation.WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Addr:              "127.0.0.1:0",
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}
	if err := app.Register(nethttp.Component("http", server)); err != nil {
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
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatalf("listener leaked after rollback: %v", err)
	}
	_ = listener.Close()
}
