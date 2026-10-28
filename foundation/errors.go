package foundation

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidConfig      = errors.New("foundation: invalid config")
	ErrInvalidComponent   = errors.New("foundation: invalid component")
	ErrDuplicateComponent = errors.New("foundation: duplicate component")
	ErrRegistrationClosed = errors.New("foundation: component registration is closed")
	ErrAlreadyRun         = errors.New("foundation: app can only run once")
	ErrNotRunning         = errors.New("foundation: app is not running")
	ErrUnexpectedExit     = errors.New("foundation: component exited unexpectedly")
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

// PanicError is returned when a lifecycle callback panics.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }
