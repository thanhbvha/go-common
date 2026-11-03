package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/foundation"
)

func awaitLifecycleRunning(t *testing.T, app *foundation.App) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if app.State() == foundation.StateRunning {
			return
		}
		if app.State() == foundation.StateFailed {
			t.Fatal("app failed before running")
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("app did not become running")
}

func TestLifecycleHTTPAndProgrammaticShutdown(t *testing.T) {
	app, server, err := newLifecycleApp("normal", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	awaitLifecycleRunning(t, app)
	// Own the connection pool and consume responses before shutdown. Closing an
	// unread body prevents reuse and can leave replacement connections racing
	// with Shutdown (which waits for new connections to send their first request).
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	for _, path := range []string{"/", "/ready", "/live"} {
		response, err := client.Get("http://" + server.Addr + path)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("%s: reading response: %v", path, errors.Join(readErr, closeErr))
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s: status=%d", path, response.StatusCode)
		}
	}
	transport.CloseIdleConnections()
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if app.State() != foundation.StateStopped {
		t.Fatal(app.State())
	}
}

func TestLifecycleFailureScenarios(t *testing.T) {
	for _, scenario := range []string{"startup-timeout", "stop-timeout", "runner-timeout", "runtime-failure", "missing", "cycle"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			app, _, err := newLifecycleApp(scenario, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- app.Run(ctx) }()
			if scenario == "stop-timeout" || scenario == "runner-timeout" {
				awaitLifecycleRunning(t, app)
				cancel()
			}
			err = <-done
			switch scenario {
			case "missing", "cycle":
				if !errors.Is(err, foundation.ErrInvalidDependency) {
					t.Fatal(err)
				}
			case "runtime-failure":
				if !errors.Is(err, errDemoRuntime) {
					t.Fatal(err)
				}
			default:
				var timeout *foundation.TimeoutError
				if !errors.As(err, &timeout) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("missing timeout: %v", err)
				}
				if timeout.Component != "worker" || len(timeout.Pending) == 0 {
					t.Fatalf("diagnostics=%+v", timeout)
				}
			}
		})
	}
}
