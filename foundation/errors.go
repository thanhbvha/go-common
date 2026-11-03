package foundation

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidConfig      = errors.New("foundation: invalid config")
	ErrInvalidComponent   = errors.New("foundation: invalid component")
	ErrDuplicateComponent = errors.New("foundation: duplicate component")
	ErrRegistrationClosed = errors.New("foundation: component registration is closed")
	ErrAlreadyRun         = errors.New("foundation: app can only run once")
	ErrNotRunning         = errors.New("foundation: app is not running")
	ErrUnexpectedExit     = errors.New("foundation: component exited unexpectedly")
	ErrInvalidDependency  = errors.New("foundation: invalid dependency graph")
)

// Phase identifies the lifecycle phase that produced an error.
type Phase string

const (
	PhaseStart Phase = "start"
	PhaseRun   Phase = "run"
	PhaseStop  Phase = "stop"
)

// ComponentError associates a lifecycle failure with its component and phase.
type ComponentError struct {
	Component string
	Phase     Phase
	Err       error
}

func (e *ComponentError) Error() string {
	return fmt.Sprintf("foundation: component %q failed during %s: %v", e.Component, e.Phase, e.Err)
}

func (e *ComponentError) Unwrap() error { return e.Err }

// TimeoutError describes lifecycle work left incomplete at a deadline.
// Pending is a snapshot in lifecycle execution order, including Component.
// Elapsed measures the whole phase; Budget is its configured upper limit (a
// parent context may expire sooner). Err retains the original deadline error.
type TimeoutError struct {
	Component string
	Phase     Phase
	Operation string
	Elapsed   time.Duration
	Budget    time.Duration
	Pending   []string
	Err       error
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("foundation: %s timeout at %q (%s), elapsed=%s budget=%s pending=%q: %v", e.Phase, e.Component, e.Operation, e.Elapsed, e.Budget, e.Pending, e.Err)
}

func (e *TimeoutError) Unwrap() error { return e.Err }

func lifecycleTimeout(err error, phase Phase, operation, component string, began time.Time, budget time.Duration, pending []Component) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	names := make([]string, len(pending))
	for i, c := range pending {
		names[i] = c.Name
	}
	return &TimeoutError{Component: component, Phase: phase, Operation: operation, Elapsed: time.Since(began), Budget: budget, Pending: names, Err: err}
}

func reverseComponents(components []Component) []Component {
	reversed := make([]Component, len(components))
	for i := range components {
		reversed[i] = components[len(components)-1-i]
	}
	return reversed
}

// PanicError is returned when a lifecycle callback panics.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }
