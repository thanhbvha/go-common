// Package httpclient provides a resilient HTTP client wrapper around go-resty.
// It features automatic Retries with exponential backoff, Circuit Breaker via gobreaker,
// and automatic OpenTelemetry W3C Trace Context propagation.
package httpclient

import (
	"time"

	"github.com/sony/gobreaker/v2"
)

// Config represents the overall configuration for the HTTP Client.
type Config struct {
	BaseURL         string
	Timeout         time.Duration
	Retry           RetryConfig
	CircuitBreaker  CBConfig
	EnableTelemetry bool
}

// RetryConfig configures the auto-retry mechanism.
type RetryConfig struct {
	Enabled     bool
	MaxRetries  int
	WaitTime    time.Duration
	MaxWaitTime time.Duration
}

// CBConfig configures the Circuit Breaker pattern.
type CBConfig struct {
	Enabled                  bool
	Name                     string
	MaxRequests              uint32
	Interval                 time.Duration
	Timeout                  time.Duration
	ReadyToTripFailRatio     float64
	ReadyToTripMinRequests   uint32
	OnStateChange            func(name string, from gobreaker.State, to gobreaker.State)
}

// DefaultConfig returns a recommended set of default configurations.
func DefaultConfig(baseURL string) Config {
	return Config{
		BaseURL:         baseURL,
		Timeout:         10 * time.Second,
		EnableTelemetry: true,
		Retry: RetryConfig{
			Enabled:     true,
			MaxRetries:  3,
			WaitTime:    1 * time.Second,
			MaxWaitTime: 5 * time.Second,
		},
		CircuitBreaker: CBConfig{
			Enabled:                true,
			Name:                   "default-cb",
			MaxRequests:            0,               // Unlimited when half-open by default (gobreaker standard: 1)
			Interval:               0,               // Cyclic period
			Timeout:                30 * time.Second, // Time in open state before transitioning to half-open
			ReadyToTripMinRequests: 5,               // Minimum requests before checking failure ratio
			ReadyToTripFailRatio:   0.6,             // Trip if > 60% failures
		},
	}
}
