package eventbus

import (
	"fmt"
	"strings"
	"time"
)

// Config configures the Event Bus.
type Config struct {
	// Namespace is used to segregate topologies for different apps sharing a broker.
	// Default: "eventbus".
	Namespace string

	// Driver is the name of the transport driver to use (e.g., "redis", "kafka", "nats").
	Driver string

	// AutoRegister automatically registers a job type if a handler is not found
	// when enqueueing. Useful for producer-only applications.
	// Default: false.
	//
	// Deprecated: declare producer-only topologies with Bus.DeclareTask or
	// Bus.DeclareEvent. AutoRegister is retained temporarily for compatibility.
	AutoRegister bool

	// ProcessTimeout is the maximum time allowed for a single job to process
	// before the context passed to the handler is canceled.
	// Default: 30s.
	ProcessTimeout time.Duration

	// Concurrency is the maximum number of concurrent workers per job type.
	// Default: 10.
	Concurrency int

	// Prefetch is the maximum number of deliveries buffered per task or
	// subscriber while all workers are busy. It excludes deliveries currently
	// executing in workers.
	// Default: 20.
	Prefetch int

	// MaxAttempts is the total number of permitted handler executions,
	// including the initial execution.
	// Default: 5.
	MaxAttempts int

	// MaxDeliveries is the legacy name for MaxAttempts. When greater than zero,
	// it overrides MaxAttempts during validation.
	//
	// Deprecated: use MaxAttempts.
	MaxDeliveries int

	// RetryPolicy defines how backoff is calculated for retries.
	// Default: ExponentialBackoff.
	RetryPolicy RetryPolicy

	// Logger receives all operational and error logs.
	Logger Logger

	// Metrics receives all operational metrics.
	Metrics Metrics

	// Tracer receives distributed tracing spans.
	Tracer Tracer
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Namespace:      DefaultNamespace,
		ProcessTimeout: 30 * time.Second,
		Concurrency:    10,
		Prefetch:       20,
		MaxAttempts:    5,
		RetryPolicy:    DefaultExponentialBackoff(),
		Logger:         noopLogger{},
		Metrics:        noopMetrics{},
		Tracer:         noopTracer{},
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.Namespace == "" {
		c.Namespace = DefaultNamespace
	} else if strings.TrimSpace(c.Namespace) == "" || c.Namespace != strings.TrimSpace(c.Namespace) {
		return fmt.Errorf("%w: namespace must not be blank or have surrounding whitespace", ErrInvalidConfig)
	}
	if c.Driver != "" && (strings.TrimSpace(c.Driver) == "" || c.Driver != strings.TrimSpace(c.Driver)) {
		return fmt.Errorf("%w: driver must not be blank or have surrounding whitespace", ErrInvalidConfig)
	}
	if c.ProcessTimeout < 0 {
		return fmt.Errorf("%w: process timeout must not be negative", ErrInvalidConfig)
	}
	if c.ProcessTimeout == 0 {
		c.ProcessTimeout = 30 * time.Second
	}
	if c.Concurrency < 0 {
		return fmt.Errorf("%w: concurrency must not be negative", ErrInvalidConfig)
	}
	if c.Concurrency == 0 {
		c.Concurrency = 10
	}
	if c.Prefetch < 0 {
		return fmt.Errorf("%w: prefetch must not be negative", ErrInvalidConfig)
	}
	if c.Prefetch == 0 {
		c.Prefetch = 20
	}
	if c.MaxAttempts < 0 {
		return fmt.Errorf("%w: max attempts must not be negative", ErrInvalidConfig)
	}
	if c.MaxDeliveries < 0 {
		return fmt.Errorf("%w: max deliveries must not be negative", ErrInvalidConfig)
	}
	if c.MaxDeliveries > 0 {
		c.MaxAttempts = c.MaxDeliveries
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 5
	}
	if c.RetryPolicy == nil {
		c.RetryPolicy = DefaultExponentialBackoff()
	}
	if c.Logger == nil {
		c.Logger = noopLogger{}
	}
	if c.Metrics == nil {
		c.Metrics = noopMetrics{}
	}
	if c.Tracer == nil {
		c.Tracer = noopTracer{}
	}
	return nil
}
