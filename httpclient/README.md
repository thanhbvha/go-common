# HTTPClient Module

The `httpclient` module provides an enterprise-grade, resilient wrapper around the popular `go-resty/resty` HTTP client. It is designed specifically for Microservices architectures, protecting your services from cascading failures when calling unreliable third-party APIs.

## Features

1. **Auto-Retry with Backoff**: Automatically retries failed requests (network errors or 5xx status codes) with Exponential Backoff.
2. **Circuit Breaker**: Integrates `sony/gobreaker`. If an external service is down and continuously returning errors, the Circuit Breaker will "trip" (Open State), failing fast for subsequent requests to prevent resource exhaustion (goroutine leaks, high memory usage) while the external service recovers.
3. **OpenTelemetry Tracing**: Automatically injects W3C Trace Context into outbound HTTP headers (`traceparent`), allowing you to trace requests seamlessly across microservice boundaries.
4. **Fluent API**: Retains the elegant, chainable API of `go-resty`.

## Installation

```bash
go get github.com/thanhbvha/go-common/httpclient
```

## Basic Usage

```go
package main

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/thanhbvha/go-common/httpclient"
)

func main() {
	// 1. Initialize Default Config
	// This enables Retry (3 attempts) and Circuit Breaker by default.
	cfg := httpclient.DefaultConfig("https://api.github.com")
	
	// 2. Create the resilient client
	client := httpclient.NewClient(cfg)

	// 3. Execute request safely
	resp, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		return req.
			SetHeader("Accept", "application/json").
			Get("/users/octocat")
	})

	if err != nil {
		// If the external service is down, the Circuit Breaker might return an OpenState error
		// instead of blocking your thread waiting for a timeout.
		fmt.Printf("Request failed: %v\n", err)
		return
	}

	fmt.Printf("Status: %d\n", resp.StatusCode())
}
```

## Configuration Options

You can deeply customize the retry and circuit breaker logic:

```go
cfg := httpclient.Config{
	BaseURL: "https://flaky-api.com",
	Timeout: 5 * time.Second,
	Retry: httpclient.RetryConfig{
		Enabled:     true,
		MaxRetries:  5,
		WaitTime:    1 * time.Second,
		MaxWaitTime: 10 * time.Second,
	},
	CircuitBreaker: httpclient.CBConfig{
		Enabled:                true,
		Name:                   "flaky-api-cb",
		Timeout:                30 * time.Second, // Stay Open for 30s before testing Half-Open
		ReadyToTripMinRequests: 10,               // Need at least 10 requests to evaluate
		ReadyToTripFailRatio:   0.5,              // Trip if 50% of requests fail
	},
}
```
