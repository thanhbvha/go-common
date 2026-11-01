package nethttp_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestComponentServesTLS(t *testing.T) {
	certificateServer := httptest.NewTLSServer(http.NewServeMux())
	certificate := certificateServer.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certificateServer.Certificate())
	certificateServer.Close()
	configs := map[string]*tls.Config{
		"certificates":    {Certificates: []tls.Certificate{certificate}},
		"get certificate": {GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &certificate, nil }},
		"get config": {GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{Certificates: []tls.Certificate{certificate}}, nil
		}},
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			server := &http.Server{
				Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second, TLSConfig: config,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.TLS == nil {
						t.Error("handler received an unencrypted request")
					}
					_, _ = io.WriteString(w, "secure")
				}),
			}
			component := nethttp.Component("https", server)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := component.Start(ctx); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- component.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
				defer stopCancel()
				if err := component.Stop(stopCtx); err != nil {
					t.Error(err)
				}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("expected canceled runner, got %v", err)
				}
			})
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			response, err := client.Get("https://" + server.Addr)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "secure" || response.TLS == nil {
				t.Fatalf("HTTPS response: body=%q TLS=%v err=%v", body, response.TLS != nil, err)
			}
			response, err = client.Get("http://" + server.Addr)
			if err == nil {
				response.Body.Close()
				if response.StatusCode != http.StatusBadRequest {
					t.Fatalf("plaintext request accepted: %s", response.Status)
				}
			}
		})
	}
}

func TestComponentRejectsUnsafeServerConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		server *http.Server
	}{
		{name: "nil handler", server: &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}},
		{name: "missing timeout", server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}},
		{name: "TLS without certificate source", server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second, TLSConfig: &tls.Config{}}},
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
