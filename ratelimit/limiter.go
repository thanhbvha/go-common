// Package ratelimit provides a distributed rate limiter backed by Redis.
// It uses Lua scripting to ensure atomic operations (Fixed Window algorithm)
// and comes with built-in middlewares for standard net/http, Fiber, Gin, and Echo.
package ratelimit

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// Config defines the rate limiting policy.
type Config struct {
	// Limit is the maximum number of requests allowed in the Window.
	Limit int

	// Window is the time frame for the limit.
	Window time.Duration
}

func (c Config) Validate() error {
	if c.Limit <= 0 || uint64(c.Limit) > (1<<53)-1 || c.Window < time.Millisecond || c.Window%time.Millisecond != 0 {
		return errors.New("ratelimit: limit must be between 1 and 2^53-1 and window must be a positive whole number of milliseconds")
	}
	return nil
}

// FailurePolicy must be selected explicitly by HTTP applications.
type FailurePolicy uint8

const (
	FailOpen FailurePolicy = iota + 1
	FailClosed
)

type MiddlewareConfig struct {
	Policy        Config
	FailurePolicy FailurePolicy
	// OnError observes backend failures; it must be fast and concurrency-safe.
	OnError func(context.Context, error)
}

func (c MiddlewareConfig) Validate() error {
	if err := c.Policy.Validate(); err != nil {
		return err
	}
	if c.FailurePolicy != FailOpen && c.FailurePolicy != FailClosed {
		return errors.New("ratelimit: choose FailOpen or FailClosed explicitly")
	}
	return nil
}

// BackendFailure reports a backend failure and returns whether to bypass it.
func (c MiddlewareConfig) BackendFailure(ctx context.Context, err error) bool {
	if c.OnError != nil {
		c.OnError(ctx, err)
	}
	return c.FailurePolicy == FailOpen
}

func ValidateMiddleware(limiter Limiter, cfg MiddlewareConfig) error {
	if limiter == nil || (reflect.ValueOf(limiter).Kind() == reflect.Ptr && reflect.ValueOf(limiter).IsNil()) {
		return errors.New("ratelimit: limiter is required")
	}
	return cfg.Validate()
}

func RetryAfterSeconds(after time.Duration) int {
	if after <= 0 {
		return 1
	}
	seconds := int(after / time.Second)
	if after%time.Second != 0 {
		seconds++
	}
	return max(1, seconds)
}

// Result contains the result of a rate limit check.
type Result struct {
	// Allowed indicates whether the request should be allowed.
	Allowed bool

	// Remaining is the number of requests left in the current window.
	Remaining int

	// ResetAfter is the time remaining until the rate limit window resets.
	ResetAfter time.Duration
}

// Limiter is the interface for rate limit implementations.
type Limiter interface {
	// Allow checks if the given key is allowed to make a request based on the config.
	// It returns a Result indicating whether the request is allowed and metadata.
	Allow(ctx context.Context, key string, cfg Config) (*Result, error)
}
