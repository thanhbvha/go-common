package httpclient

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/sony/gobreaker/v2"
)

// Client is a wrapper around resty.Client that provides Circuit Breaking and Telemetry.
type Client struct {
	restyClient *resty.Client
	cb          *gobreaker.CircuitBreaker[[]byte]
	cfg         Config
}

// NewClient creates a new resilient HTTP Client.
func NewClient(cfg Config) *Client {
	rc := resty.New()
	rc.SetBaseURL(cfg.BaseURL)
	rc.SetTimeout(cfg.Timeout)

	if cfg.Retry.Enabled {
		rc.SetRetryCount(cfg.Retry.MaxRetries)
		rc.SetRetryWaitTime(cfg.Retry.WaitTime)
		rc.SetRetryMaxWaitTime(cfg.Retry.MaxWaitTime)
		// Resty will automatically retry on connection errors or 5xx status codes.
		rc.AddRetryCondition(func(r *resty.Response, err error) bool {
			return err != nil || r.StatusCode() >= 500
		})
	}

	if cfg.EnableTelemetry {
		rc.OnBeforeRequest(telemetryOnBeforeRequest)
		rc.OnAfterResponse(telemetryOnAfterResponse)
	}

	cb := setupCircuitBreaker(cfg.CircuitBreaker)

	return &Client{
		restyClient: rc,
		cb:          cb,
		cfg:         cfg,
	}
}

// Native returns the underlying resty.Client in case advanced configuration is needed.
func (c *Client) Native() *resty.Client {
	return c.restyClient
}

// Execute performs an HTTP request wrapped in a Circuit Breaker (if enabled).
// The provided function `fn` describes how to actually build and send the request using the Resty client.
func (c *Client) Execute(ctx context.Context, fn func(req *resty.Request) (*resty.Response, error)) (*resty.Response, error) {
	req := c.restyClient.R().SetContext(ctx)

	if c.cb == nil {
		// Circuit Breaker disabled
		return fn(req)
	}

	// Circuit Breaker enabled.
	// Since gobreaker works with generic types, we execute it and return dummy bytes,
	// capturing the actual response in a closure variable.
	var finalResp *resty.Response
	
	_, err := c.cb.Execute(func() ([]byte, error) {
		resp, reqErr := fn(req)
		finalResp = resp
		
		// Determine if the circuit breaker should count this as a failure.
		if reqErr != nil {
			return nil, reqErr
		}
		
		if resp != nil && resp.StatusCode() >= 500 {
			return nil, fmt.Errorf("server error: status code %d", resp.StatusCode())
		}
		
		return nil, nil // Success
	})

	// Return the captured response and the error (which might be gobreaker.ErrOpenState)
	return finalResp, err
}
