package foundation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/v2/health"
)

var errShutdownRequested = errors.New("foundation: shutdown requested")

type runResult struct {
	component  string
	err        error
	contextErr error
}

// App supervises registered components through startup, runtime, and shutdown.
// App is safe for concurrent State and Shutdown calls. Register must complete
// before Run begins.
type App struct {
	cfg     Config
	logger  Logger
	health  *health.Checker
	signals []os.Signal

	mu         sync.Mutex
	state      State
	components []Component
	names      map[string]struct{}
	cancel     context.CancelCauseFunc
	done       chan struct{}
	result     error
}

// New creates a validated application lifecycle manager.
func New(cfg Config, options ...Option) (*App, error) {
	name, err := normalizedName(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: name %v", ErrInvalidConfig, err)
	}
	cfg.Name = name
	if cfg.Version != "" {
		version, err := normalizedName(cfg.Version)
		if err != nil {
			return nil, fmt.Errorf("%w: version %v", ErrInvalidConfig, err)
		}
		cfg.Version = version
	}
	if cfg.StartupTimeout < 0 || cfg.ShutdownTimeout < 0 {
		return nil, fmt.Errorf("%w: lifecycle timeouts must not be negative", ErrInvalidConfig)
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = defaultStartupTimeout
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	settings := defaultSettings(cfg.Version)
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option must not be nil", ErrInvalidConfig)
		}
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	app := &App{
		cfg:     cfg,
		logger:  settings.logger,
		health:  settings.health,
		signals: append([]os.Signal(nil), settings.signals...),
		state:   StateCreated,
		names:   make(map[string]struct{}),
		done:    make(chan struct{}),
	}
	app.health.AddReadinessCheck(lifecycleCheckName, func(context.Context) error {
		if state := app.State(); state != StateRunning {
			return fmt.Errorf("application is %s", state)
		}
		return nil
	})
	return app, nil
}

// Register adds a component before Run begins.
func (a *App) Register(component Component) error {
	name, err := normalizedName(component.Name)
	if err != nil {
		return fmt.Errorf("%w: component name %v", ErrInvalidComponent, err)
	}
	component.Name = name
	if component.Start == nil && component.Run == nil && component.Stop == nil {
		return fmt.Errorf("%w: at least one callback is required", ErrInvalidComponent)
	}
	dependencies := make([]string, len(component.DependsOn))
	seen := make(map[string]bool, len(dependencies))
	for i, dependency := range component.DependsOn {
		dependency, err = normalizedName(dependency)
		if err != nil {
			return fmt.Errorf("%w: dependency name %v", ErrInvalidComponent, err)
		}
		if dependency == name || seen[dependency] {
			return fmt.Errorf("%w: self or duplicate dependency %q for %q", ErrInvalidComponent, dependency, name)
		}
		seen[dependency] = true
		dependencies[i] = dependency
	}
	component.DependsOn = dependencies

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != StateCreated {
		return ErrRegistrationClosed
	}
	if _, exists := a.names[component.Name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateComponent, component.Name)
	}
	a.names[component.Name] = struct{}{}
	a.components = append(a.components, component)
	return nil
}

// State returns the current lifecycle state.
func (a *App) State() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

// Health returns the application health checker. The reserved readiness check
// "foundation.lifecycle" reflects the current lifecycle state.
func (a *App) Health() *health.Checker {
	return a.health
}

// Run starts every component, supervises runners, and blocks until cancellation,
// an OS signal, Shutdown, or a runtime failure. An App can only run once.
func (a *App) Run(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("%w: run context must not be nil", ErrInvalidConfig)
	}

	a.mu.Lock()
	if a.state != StateCreated {
		a.mu.Unlock()
		return ErrAlreadyRun
	}
	a.state = StateStarting
	components := append([]Component(nil), a.components...)
	a.mu.Unlock()
	components, err := orderComponents(components)
	if err != nil {
		a.finish(err)
		return err
	}

	var stopSignals context.CancelFunc = func() {}
	if len(a.signals) > 0 {
		parent, stopSignals = signal.NotifyContext(parent, a.signals...)
	}
	defer stopSignals()

	runCtx, cancel := context.WithCancelCause(parent)
	a.mu.Lock()
	a.cancel = cancel
	shutdownRequested := a.state == StateStopping
	a.mu.Unlock()
	if shutdownRequested {
		cancel(errShutdownRequested)
	}

	a.logInfo("application starting", "name", a.cfg.Name, "version", a.cfg.Version)
	started, primaryErr := a.start(runCtx, components)
	if primaryErr != nil {
		cancel(primaryErr)
	}

	runResults := make(chan runResult, len(started))
	runnerDone := make(map[string]<-chan struct{}, len(started))
	if primaryErr == nil && runCtx.Err() == nil {
		for _, component := range started {
			if component.Run == nil {
				continue
			}
			done := make(chan struct{})
			runnerDone[component.Name] = done
			go a.runComponent(runCtx, component, runResults, done)
		}
		select {
		case result := <-runResults:
			primaryErr = runtimeComponentError(result)
			if primaryErr != nil {
				cancel(primaryErr)
			}
		default:
			if a.transitionToRunning(runCtx) {
				a.logInfo("application started", "name", a.cfg.Name)
				select {
				case result := <-runResults:
					primaryErr = runtimeComponentError(result)
					if primaryErr != nil {
						cancel(primaryErr)
					}
				case <-runCtx.Done():
				}
			} else {
				cancel(errShutdownRequested)
			}
		}
	}

	a.setState(StateStopping)
	cancel(errShutdownRequested)
	a.logInfo("application stopping", "name", a.cfg.Name)
	shutdownErr := a.stop(started, runnerDone)
	// Results are buffered before runners signal completion. Collect every
	// available failure, including errors produced while draining on cancellation.
collectResults:
	for {
		select {
		case result := <-runResults:
			primaryErr = errors.Join(primaryErr, runtimeComponentError(result))
		default:
			break collectResults
		}
	}
	result := errors.Join(primaryErr, shutdownErr)
	a.finish(result)
	return result
}

// Shutdown requests graceful shutdown and waits for Run to finish. Concurrent
// calls are safe and observe the same terminal result.
func (a *App) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context must not be nil", ErrInvalidConfig)
	}

	a.mu.Lock()
	switch a.state {
	case StateCreated:
		a.mu.Unlock()
		return ErrNotRunning
	case StateStarting, StateRunning:
		a.state = StateStopping
	}
	cancel := a.cancel
	done := a.done
	if a.state == StateStopped || a.state == StateFailed {
		result := a.result
		a.mu.Unlock()
		return result
	}
	a.mu.Unlock()

	if cancel != nil {
		cancel(errShutdownRequested)
	}
	select {
	case <-done:
		a.mu.Lock()
		result := a.result
		a.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) start(runCtx context.Context, components []Component) ([]Component, error) {
	began := time.Now()
	startupCtx, cancel := context.WithTimeout(runCtx, a.cfg.StartupTimeout)
	defer cancel()

	started := make([]Component, 0, len(components))
	for i, component := range components {
		if err := startupCtx.Err(); err != nil {
			if runCtx.Err() != nil {
				return started, nil
			}
			return started, &ComponentError{Component: component.Name, Phase: PhaseStart, Err: lifecycleTimeout(err, PhaseStart, "before start", component.Name, began, a.cfg.StartupTimeout, components[i:])}
		}
		if component.Start != nil {
			err, pending := callBounded(startupCtx, component.Start)
			if err != nil {
				if pending != nil {
					a.cleanupLateStart(component, pending)
				}
				if runCtx.Err() != nil {
					return started, nil
				}
				return started, &ComponentError{Component: component.Name, Phase: PhaseStart, Err: lifecycleTimeout(err, PhaseStart, "start callback", component.Name, began, a.cfg.StartupTimeout, components[i:])}
			}
		}
		started = append(started, component)
	}
	return started, nil
}

func (a *App) runComponent(ctx context.Context, component Component, results chan<- runResult, done chan<- struct{}) {
	defer close(done)
	err := call(component.Run, ctx)
	results <- runResult{component: component.Name, err: err, contextErr: ctx.Err()}
}

func (a *App) stop(started []Component, runnerDone map[string]<-chan struct{}) error {
	began := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		if shutdownCtx.Err() != nil {
			errs = append(errs, &ComponentError{Component: started[i].Name, Phase: PhaseStop, Err: lifecycleTimeout(shutdownCtx.Err(), PhaseStop, "before stop", started[i].Name, began, a.cfg.ShutdownTimeout, reverseComponents(started[:i+1]))})
			break
		}
		component := started[i]
		if component.Stop != nil {
			err, _ := callBounded(shutdownCtx, component.Stop)
			if err != nil {
				errs = append(errs, &ComponentError{Component: component.Name, Phase: PhaseStop, Err: lifecycleTimeout(err, PhaseStop, "stop callback", component.Name, began, a.cfg.ShutdownTimeout, reverseComponents(started[:i+1]))})
			}
			if shutdownCtx.Err() != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					errs = append(errs, &ComponentError{Component: component.Name, Phase: PhaseStop, Err: lifecycleTimeout(shutdownCtx.Err(), PhaseStop, "after stop callback", component.Name, began, a.cfg.ShutdownTimeout, reverseComponents(started[:i+1]))})
				}
				break
			}
		}
		// A worker without a Stop callback still has to finish before an
		// earlier dependency is released.
		if done := runnerDone[component.Name]; done != nil {
			select {
			case <-done:
			case <-shutdownCtx.Done():
				errs = append(errs, &ComponentError{
					Component: component.Name,
					Phase:     PhaseStop,
					Err:       lifecycleTimeout(shutdownCtx.Err(), PhaseStop, "runner wait", component.Name, began, a.cfg.ShutdownTimeout, reverseComponents(started[:i+1])),
				})
				return errors.Join(errs...)
			}
		}
	}
	return errors.Join(errs...)
}

func (a *App) setState(state State) {
	a.mu.Lock()
	a.state = state
	a.mu.Unlock()
}

func (a *App) transitionToRunning(ctx context.Context) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != StateStarting || ctx.Err() != nil {
		return false
	}
	a.state = StateRunning
	return true
}

func (a *App) finish(result error) {
	a.mu.Lock()
	a.result = result
	if result != nil {
		a.state = StateFailed
	} else {
		a.state = StateStopped
	}
	close(a.done)
	a.mu.Unlock()

	if result != nil {
		a.logError("application stopped with errors", "name", a.cfg.Name, "error", result)
		return
	}
	a.logInfo("application stopped", "name", a.cfg.Name)
}

func callBounded(ctx context.Context, callback func(context.Context) error) (error, <-chan error) {
	result := make(chan error, 1)
	go func() { result <- call(callback, ctx) }()
	select {
	case err := <-result:
		return err, nil
	case <-ctx.Done():
		return ctx.Err(), result
	}
}

func (a *App) cleanupLateStart(component Component, pending <-chan error) {
	if component.Stop == nil {
		return
	}
	go func() {
		if err := <-pending; err != nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
		defer cancel()
		if err, _ := callBounded(cleanupCtx, component.Stop); err != nil {
			a.logError("late-start cleanup failed", "component", component.Name, "error", err)
		}
	}()
}

func runtimeComponentError(result runResult) error {
	if result.contextErr != nil && (result.err == nil || cancellationOnly(result.err, result.contextErr)) {
		return nil
	}
	if result.err == nil {
		result.err = ErrUnexpectedExit
	}
	return &ComponentError{Component: result.component, Phase: PhaseRun, Err: result.err}
}

// A joined cancellation and a real failure must still be reported. Walk error
// trees rather than using errors.Is, which matches any child of errors.Join.
func cancellationOnly(err, expected error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !cancellationOnly(child, expected) {
				return false
			}
		}
		return true
	}
	if wrapped := errors.Unwrap(err); wrapped != nil {
		return cancellationOnly(wrapped, expected)
	}
	return err == expected
}

func (a *App) logInfo(message string, args ...any) {
	safeLog(func() { a.logger.Info(message, args...) })
}

func (a *App) logError(message string, args ...any) {
	safeLog(func() { a.logger.Error(message, args...) })
}

func safeLog(log func()) {
	defer func() { _ = recover() }()
	log()
}

func call(callback func(context.Context) error, ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return callback(ctx)
}
