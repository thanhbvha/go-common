package foundation

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTimeoutDiagnostics(t *testing.T) {
	for _, operation := range []string{"start callback", "stop callback", "runner wait"} {
		t.Run(operation, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			blocked := func(context.Context) error { <-release; return nil }
			app := newTestApp(t, Config{Name: "timeout", StartupTimeout: 30 * time.Millisecond, ShutdownTimeout: 30 * time.Millisecond})
			var c Component
			var wantPending []string
			phase := PhaseStop
			if operation == "start callback" {
				phase = PhaseStart
				c = Component{Name: "blocked", Start: blocked}
				wantPending = []string{"blocked", "later"}
			} else {
				c = Component{Name: "blocked", DependsOn: []string{"later"}}
				if operation == "stop callback" {
					c.Stop = blocked
				} else {
					c.Run = blocked
				}
				wantPending = []string{"blocked", "later"}
			}
			if err := app.Register(c); err != nil {
				t.Fatal(err)
			}
			if err := app.Register(Resource("later", nil, func(context.Context) error { return nil })); err != nil {
				t.Fatal(err)
			}
			var err error
			if phase == PhaseStart {
				err = app.Run(t.Context())
			} else {
				done := runAsync(app, t.Context())
				waitForState(t, app, StateRunning)
				err = app.Shutdown(t.Context())
				if result := <-done; result != err {
					t.Fatal("terminal result differs")
				}
			}
			var timeout *TimeoutError
			if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &timeout) {
				t.Fatalf("expected deadline diagnostics: %v", err)
			}
			if timeout.Component != "blocked" || timeout.Phase != phase || timeout.Operation != operation || timeout.Budget != 30*time.Millisecond || timeout.Elapsed <= 0 || !equalStrings(timeout.Pending, wantPending) {
				t.Fatalf("unexpected diagnostics: %+v", timeout)
			}
			assertComponentError(t, err, "blocked", phase)
		})
	}
}

func TestShutdownDeadlineAfterSuccessfulCallbackIsReported(t *testing.T) {
	app := newTestApp(t, Config{Name: "deadline", ShutdownTimeout: 20 * time.Millisecond})
	if err := app.Register(Resource("resource", nil, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	done := runAsync(app, t.Context())
	waitForState(t, app, StateRunning)
	err := app.Shutdown(t.Context())
	<-done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline disappeared: %v", err)
	}
}
