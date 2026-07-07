package ratelimit

import (
	"context"
	"time"
)

// Config defines the rate limiting policy.
type Config struct {
	// Limit is the maximum number of requests allowed in the Window.
	Limit int

	// Window is the time frame for the limit.
	Window time.Duration
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
