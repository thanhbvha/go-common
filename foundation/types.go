// Package foundation provides a supervised application lifecycle for Go services.
//
// Components start in registration order, run concurrently, and stop in reverse
// registration order. Runtime failures trigger graceful shutdown and are returned
// to the caller instead of being lost in background goroutines.
package foundation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/thanhbvha/go-common/health"
)

const (
	defaultStartupTimeout  = 30 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	lifecycleCheckName     = "foundation.lifecycle"
	maxNameBytes           = 128
)

// Config defines application identity and lifecycle deadlines.
type Config struct {
	Name            string
	Version         string
	StartupTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// Component is one supervised application unit. All callbacks are optional,
// but at least one must be provided.
type Component struct {
	Name  string
	Start func(context.Context) error
	Run   func(context.Context) error
	Stop  func(context.Context) error
}

// State represents the current application lifecycle state.
type State uint8

const (
	StateCreated State = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	case StateFailed:
		return "failed"
	default:
		return fmt.Sprintf("state(%d)", s)
	}
}

// Logger is the minimal structured logging contract used by App.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type settings struct {
	logger  Logger
	health  *health.Checker
	signals []os.Signal
}

// Option customizes an App.
type Option func(*settings) error

// WithLogger uses logger for lifecycle messages.
func WithLogger(logger Logger) Option {
	return func(s *settings) error {
		if isNil(logger) {
			return fmt.Errorf("%w: logger must not be nil", ErrInvalidConfig)
		}
		s.logger = logger
		return nil
	}
}

func normalizedName(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("must not be empty")
	}
	if len(value) > maxNameBytes {
		return "", fmt.Errorf("must not exceed %d bytes", maxNameBytes)
	}
	for _, character := range value {
		if unicode.Is(unicode.C, character) {
			return "", errors.New("must not contain control characters")
		}
	}
	return value, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// WithHealthChecker uses checker for dependency and lifecycle health checks.
func WithHealthChecker(checker *health.Checker) Option {
	return func(s *settings) error {
		if checker == nil {
			return fmt.Errorf("%w: health checker must not be nil", ErrInvalidConfig)
		}
		s.health = checker
		return nil
	}
}

// WithSignals replaces the default SIGINT/SIGTERM signal set.
func WithSignals(signals ...os.Signal) Option {
	return func(s *settings) error {
		if len(signals) == 0 {
			return fmt.Errorf("%w: at least one signal is required", ErrInvalidConfig)
		}
		for _, signalValue := range signals {
			if isNil(signalValue) {
				return fmt.Errorf("%w: signals must not contain nil", ErrInvalidConfig)
			}
		}
		s.signals = append([]os.Signal(nil), signals...)
		return nil
	}
}

// WithoutSignals disables OS signal handling. Context cancellation and Shutdown
// remain available, which is useful for tests and embedded applications.
func WithoutSignals() Option {
	return func(s *settings) error {
		s.signals = nil
		return nil
	}
}

func defaultSettings(version string) settings {
	return settings{
		logger:  slog.Default(),
		health:  health.New().WithVersion(version),
		signals: []os.Signal{os.Interrupt, syscall.SIGTERM},
	}
}
