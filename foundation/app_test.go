package foundation

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/health"
)

func TestNewValidatesConfigAndOptions(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid config, got %v", err)
	}
	if _, err := New(Config{Name: "app", StartupTimeout: -time.Second}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid timeout, got %v", err)
	}
	if _, err := New(Config{Name: "app"}, WithLogger(nil)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid logger, got %v", err)
	}
	if _, err := New(Config{Name: "app"}, WithHealthChecker(nil)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid health checker, got %v", err)
	}
	if _, err := New(Config{Name: "app"}, WithSignals()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid signals, got %v", err)
	}

	app, err := New(Config{Name: " app "}, WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	if app.cfg.StartupTimeout != defaultStartupTimeout || app.cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("unexpected defaults: %+v", app.cfg)
	}
	if app.State() != StateCreated {
		t.Fatalf("expected created, got %s", app.State())
	}
	if got := app.Health().CheckReadiness(t.Context()).Status; got != health.StatusDown {
		t.Fatalf("expected readiness down before run, got %s", got)
	}
}

func TestRegisterValidationAndFreeze(t *testing.T) {
	app := newTestApp(t, Config{Name: "app"})
	if err := app.Register(Component{}); !errors.Is(err, ErrInvalidComponent) {
		t.Fatalf("expected invalid component, got %v", err)
	}
	if err := app.Register(Worker("worker", waitForCancellation)); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(Worker("worker", waitForCancellation)); !errors.Is(err, ErrDuplicateComponent) {
		t.Fatalf("expected duplicate component, got %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(app, ctx)
	waitForState(t, app, StateRunning)
	if err := app.Register(Worker("late", waitForCancellation)); !errors.Is(err, ErrRegistrationClosed) {
		t.Fatalf("expected registration closed, got %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleOrderAndReadiness(t *testing.T) {
	app := newTestApp(t, Config{Name: "app"})
	var order []string
	for _, name := range []string{"database", "server"} {
		name := name
		if err := app.Register(Component{
			Name: name,
			Start: func(context.Context) error {
				order = append(order, "start:"+name)
				return nil
			},
			Run: waitForCancellation,
			Stop: func(context.Context) error {
				order = append(order, "stop:"+name)
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(app, ctx)
	waitForState(t, app, StateRunning)
	if got := app.Health().CheckReadiness(t.Context()).Status; got != health.StatusOK {
		t.Fatalf("expected ready while running, got %s", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	want := []string{"start:database", "start:server", "stop:server", "stop:database"}
	if !equalStrings(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if app.State() != StateStopped {
		t.Fatalf("expected stopped, got %s", app.State())
	}
	if got := app.Health().CheckReadiness(t.Context()).Status; got != health.StatusDown {
		t.Fatalf("expected not ready after stop, got %s", got)
	}
}

func TestStartupFailureRollsBackStartedComponents(t *testing.T) {
	startErr := errors.New("database unavailable")
	app := newTestApp(t, Config{Name: "app"})
	var stopped atomic.Bool
	if err := app.Register(Resource("first", nil, func(context.Context) error {
		stopped.Store(true)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(Resource("second", func(context.Context) error { return startErr }, nil)); err != nil {
		t.Fatal(err)
	}

	err := app.Run(t.Context())
	if !errors.Is(err, startErr) {
		t.Fatalf("expected startup error, got %v", err)
	}
	if !stopped.Load() {
		t.Fatal("expected first component to be rolled back")
	}
	assertComponentError(t, err, "second", PhaseStart)
}

func TestRuntimeFailureTriggersShutdown(t *testing.T) {
	runtimeErr := errors.New("listener failed")
	app := newTestApp(t, Config{Name: "app"})
	var stopped atomic.Bool
	if err := app.Register(Component{
		Name: "server",
		Run:  func(context.Context) error { return runtimeErr },
		Stop: func(context.Context) error { stopped.Store(true); return nil },
	}); err != nil {
		t.Fatal(err)
	}
	err := app.Run(t.Context())
	if !errors.Is(err, runtimeErr) || !stopped.Load() {
		t.Fatalf("expected runtime error and shutdown, got err=%v stopped=%v", err, stopped.Load())
	}
	assertComponentError(t, err, "server", PhaseRun)

	unexpected := newTestApp(t, Config{Name: "unexpected"})
	if err := unexpected.Register(Worker("worker", func(context.Context) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := unexpected.Run(t.Context()); !errors.Is(err, ErrUnexpectedExit) {
		t.Fatalf("expected unexpected exit, got %v", err)
	}
}

func TestPanicsBecomeComponentErrors(t *testing.T) {
	tests := []struct {
		name      string
		component Component
		phase     Phase
		cancel    bool
	}{
		{name: "start", component: Component{Name: "panic", Start: panicCallback}, phase: PhaseStart},
		{name: "run", component: Component{Name: "panic", Run: panicCallback}, phase: PhaseRun},
		{name: "stop", component: Component{Name: "panic", Run: waitForCancellation, Stop: panicCallback}, phase: PhaseStop, cancel: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newTestApp(t, Config{Name: test.name})
			if err := app.Register(test.component); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			if test.cancel {
				go func() {
					waitForState(t, app, StateRunning)
					cancel()
				}()
			}
			err := app.Run(ctx)
			cancel()
			assertComponentError(t, err, "panic", test.phase)
			var panicErr *PanicError
			if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
				t.Fatalf("expected panic error with stack, got %v", err)
			}
		})
	}
}

func TestLifecycleTimeoutsBoundUncooperativeCallbacks(t *testing.T) {
	t.Run("startup", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		app := newTestApp(t, Config{Name: "startup", StartupTimeout: 20 * time.Millisecond})
		if err := app.Register(Resource("blocked", func(context.Context) error { <-release; return nil }, nil)); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		err := app.Run(t.Context())
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("startup was not bounded: err=%v duration=%s", err, time.Since(started))
		}
	})

	t.Run("shutdown preserves earlier dependency after timeout", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		fastCalled := make(chan struct{}, 1)
		app := newTestApp(t, Config{Name: "shutdown", ShutdownTimeout: 20 * time.Millisecond})
		if err := app.Register(Resource("fast", nil, func(context.Context) error {
			fastCalled <- struct{}{}
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		if err := app.Register(Component{Name: "blocked", Run: waitForCancellation, Stop: func(context.Context) error {
			<-release
			return nil
		}}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(app, ctx)
		waitForState(t, app, StateRunning)
		cancel()
		err := <-done
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected shutdown deadline, got %v", err)
		}
		select {
		case <-fastCalled:
			t.Fatal("earlier dependency must remain open after dependent stop timeout")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("runtime", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		app := newTestApp(t, Config{Name: "runtime", ShutdownTimeout: 20 * time.Millisecond})
		if err := app.Register(Worker("blocked", func(context.Context) error {
			<-release
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(app, ctx)
		waitForState(t, app, StateRunning)
		cancel()
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected runner shutdown deadline, got %v", err)
		}
	})
}

func TestLateStartupSuccessIsCleanedUp(t *testing.T) {
	release := make(chan struct{})
	cleaned := make(chan struct{})
	app := newTestApp(t, Config{
		Name:            "late-start",
		StartupTimeout:  20 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})
	if err := app.Register(Resource("resource", func(context.Context) error {
		<-release
		return nil
	}, func(context.Context) error {
		close(cleaned)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := app.Run(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected startup deadline, got %v", err)
	}
	close(release)
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("late startup success was not cleaned up")
	}
}

func TestLoggerAndNameHardening(t *testing.T) {
	var typedNil *panicLogger
	if _, err := New(Config{Name: "app"}, WithLogger(typedNil)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected typed-nil logger rejection, got %v", err)
	}
	if _, err := New(Config{Name: "bad\nname"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected control-character name rejection, got %v", err)
	}
	if _, err := New(Config{Name: "bad\u202ename"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected Unicode format-character name rejection, got %v", err)
	}
	if _, err := New(Config{Name: "app", Version: "bad\nversion"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected control-character version rejection, got %v", err)
	}

	app, err := New(Config{Name: "app"}, WithLogger(&panicLogger{}), WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Register(Worker("worker", waitForCancellation)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(app, ctx)
	waitForState(t, app, StateRunning)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("panicking logger affected lifecycle: %v", err)
	}
}

func TestShutdownJoinsAllErrors(t *testing.T) {
	firstErr := errors.New("first stop failed")
	secondErr := errors.New("second stop failed")
	app := newTestApp(t, Config{Name: "errors"})
	if err := app.Register(Resource("first", nil, func(context.Context) error { return firstErr })); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(Resource("second", nil, func(context.Context) error { return secondErr })); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(app, ctx)
	waitForState(t, app, StateRunning)
	cancel()
	err := <-done
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("expected both shutdown errors, got %v", err)
	}
}

func TestShutdownJoinsDependentRunnerBeforeReleasingResource(t *testing.T) {
	app := newTestApp(t, Config{Name: "dependencies"})
	workerExited := make(chan struct{})
	if err := app.Register(Resource("database", nil, func(context.Context) error {
		select {
		case <-workerExited:
			return nil
		default:
			return errors.New("database closed before worker exited")
		}
	})); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(Worker("worker", func(ctx context.Context) error {
		defer close(workerExited)
		<-ctx.Done()
		return ctx.Err()
	})); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(app, ctx)
	waitForState(t, app, StateRunning)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("dependency shutdown order failed: %v", err)
	}
}

func TestConcurrentShutdownAndRunOnce(t *testing.T) {
	app := newTestApp(t, Config{Name: "app"})
	if err := app.Register(Worker("worker", waitForCancellation)); err != nil {
		t.Fatal(err)
	}
	runDone := runAsync(app, t.Context())
	waitForState(t, app, StateRunning)

	const callers = 12
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results <- app.Shutdown(ctx)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("shutdown returned %v", err)
		}
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := app.Run(t.Context()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("expected run-once error, got %v", err)
	}
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatalf("terminal shutdown returned %v", err)
	}
}

func TestShutdownCannotTransitionBackToRunning(t *testing.T) {
	app := newTestApp(t, Config{Name: "state-race"})
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	if err := app.Register(Component{
		Name: "worker",
		Start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		},
		Run: waitForCancellation,
	}); err != nil {
		t.Fatal(err)
	}

	runDone := runAsync(app, t.Context())
	<-startEntered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- app.Shutdown(t.Context()) }()
	deadline := time.Now().Add(time.Second)
	observedShutdown := false
	for time.Now().Before(deadline) {
		state := app.State()
		if state == StateRunning {
			t.Fatal("shutdown state was overwritten by running")
		}
		if state == StateStopping || state == StateStopped {
			observedShutdown = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !observedShutdown {
		t.Fatalf("shutdown transition was not observed; state=%s", app.State())
	}
	close(releaseStart)

	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if app.State() != StateStopped {
		t.Fatalf("expected stopped, got %s", app.State())
	}
	if result := app.Health().CheckReadiness(t.Context()); result.Status != health.StatusDown {
		t.Fatalf("readiness returned to healthy after shutdown: %+v", result)
	}
}

func TestHelpers(t *testing.T) {
	closer := &testCloser{}
	component := Closer("closer", closer)
	if component.Name != "closer" || component.Stop == nil {
		t.Fatal("unexpected closer component")
	}
	if err := component.Stop(t.Context()); err != nil || !closer.closed {
		t.Fatalf("closer failed: err=%v closed=%v", err, closer.closed)
	}
	if got := Worker("worker", waitForCancellation); got.Run == nil || got.Name != "worker" {
		t.Fatal("unexpected worker component")
	}
	if got := Resource("resource", nil, nil); got.Name != "resource" {
		t.Fatal("unexpected resource component")
	}
}

type testCloser struct{ closed bool }

func (c *testCloser) Close() error { c.closed = true; return nil }

var _ io.Closer = (*testCloser)(nil)

type panicLogger struct{}

func (*panicLogger) Info(string, ...any)  { panic("logger panic") }
func (*panicLogger) Error(string, ...any) { panic("logger panic") }

func newTestApp(t *testing.T, cfg Config) *App {
	t.Helper()
	app, err := New(cfg, WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func runAsync(app *App, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	return done
}

func waitForState(t *testing.T, app *App, state State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if app.State() == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state did not become %s; current=%s", state, app.State())
}

func waitForCancellation(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func panicCallback(context.Context) error { panic("boom") }

func assertComponentError(t *testing.T, err error, name string, phase Phase) {
	t.Helper()
	var componentErr *ComponentError
	if !errors.As(err, &componentErr) {
		t.Fatalf("expected ComponentError, got %v", err)
	}
	if componentErr.Component != name || componentErr.Phase != phase {
		t.Fatalf("unexpected component error: %+v", componentErr)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
