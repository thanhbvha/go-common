package foundation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"

	"github.com/thanhbvha/go-common/health"
)

var errShutdownRequested = errors.New("foundation: shutdown requested")

type runResult struct {
	component string
	err       error
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
			cancel(primaryErr)
		default:
			if a.transitionToRunning(runCtx) {
				a.logInfo("application started", "name", a.cfg.Name)
				select {
				case result := <-runResults:
					if runCtx.Err() == nil {
						primaryErr = runtimeComponentError(result)
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
	startupCtx, cancel := context.WithTimeout(runCtx, a.cfg.StartupTimeout)
	defer cancel()

	started := make([]Component, 0, len(components))
	for _, component := range components {
		if err := startupCtx.Err(); err != nil {
			if runCtx.Err() != nil {
				return started, nil
			}
			return started, &ComponentError{Component: component.Name, Phase: PhaseStart, Err: err}
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
				return started, &ComponentError{Component: component.Name, Phase: PhaseStart, Err: err}
			}
		}
		started = append(started, component)
	}
	return started, nil
}

func (a *App) runComponent(ctx context.Context, component Component, results chan<- runResult, done chan<- struct{}) {
	defer close(done)
	results <- runResult{component: component.Name, err: call(component.Run, ctx)}
}

func (a *App) stop(started []Component, runnerDone map[string]<-chan struct{}) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		if shutdownCtx.Err() != nil {
			break
		}
		component := started[i]
		if component.Stop != nil {
			if err, _ := callBounded(shutdownCtx, component.Stop); err != nil {
				errs = append(errs, &ComponentError{Component: component.Name, Phase: PhaseStop, Err: err})
			}
			if shutdownCtx.Err() != nil {
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
					Err:       fmt.Errorf("runner did not stop: %w", shutdownCtx.Err()),
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
	if result.err == nil {
		result.err = ErrUnexpectedExit
	}
	return &ComponentError{Component: result.component, Phase: PhaseRun, Err: result.err}
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
