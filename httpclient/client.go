package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sony/gobreaker/v2"
)

var errServerStatus = errors.New("httpclient: server error")

type retryKey struct{}

// WithIdempotentRetry opts an operation into retries for unsafe HTTP methods.
// The application must ensure the server supports idempotency (for example with
// a stable Idempotency-Key). Streaming and multipart bodies are never retried.
func WithIdempotentRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryKey{}, true)
}

// Client reuses a Resty connection pool with bounded retries and optional tracing.
// Build and configure Native before using the client concurrently.
// HTTP 4xx/5xx remain responses with nil error, regardless of breaker settings.
type Client struct {
	restyClient *resty.Client
	cb          *gobreaker.TwoStepCircuitBreaker[any]
	cfg         Config
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.Timeout < 0 || cfg.Retry.MaxRetries < 0 || cfg.Retry.WaitTime < 0 || cfg.Retry.MaxWaitTime < 0 || cfg.MaxResponseBodyBytes < 0 {
		return nil, errors.New("httpclient: invalid timeout, retry, or body limit")
	}
	if cfg.BaseURL != "" {
		parsed, err := url.Parse(cfg.BaseURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" || strings.Contains(cfg.BaseURL, "#") {
			return nil, errors.New("httpclient: base URL must be an HTTP(S) origin or base path without credentials, query, or fragment")
		}
	}
	if cfg.CircuitBreaker.Enabled && (math.IsNaN(cfg.CircuitBreaker.ReadyToTripFailRatio) || math.IsInf(cfg.CircuitBreaker.ReadyToTripFailRatio, 0) || cfg.CircuitBreaker.ReadyToTripFailRatio <= 0 || cfg.CircuitBreaker.ReadyToTripFailRatio > 1 || cfg.CircuitBreaker.Interval < 0 || cfg.CircuitBreaker.Timeout < 0) {
		return nil, errors.New("httpclient: invalid circuit breaker configuration")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxResponseBodyBytes == 0 {
		cfg.MaxResponseBodyBytes = 4 << 20
	}
	if cfg.Retry.WaitTime == 0 {
		cfg.Retry.WaitTime = 100 * time.Millisecond
	}
	if cfg.Retry.MaxWaitTime == 0 {
		cfg.Retry.MaxWaitTime = time.Second
	}
	if cfg.Retry.MaxWaitTime < cfg.Retry.WaitTime {
		return nil, errors.New("httpclient: max retry wait must be at least retry wait")
	}
	rc := resty.New().SetBaseURL(cfg.BaseURL).SetResponseBodyLimit(cfg.MaxResponseBodyBytes)
	// Resty's retry logger includes raw URLs in transport errors. Applications
	// own diagnostic logging; transport spans expose only sanitized metadata.
	rc.SetLogger(quietLogger{})
	base := cfg.Transport
	if base == nil {
		if native, ok := http.DefaultTransport.(*http.Transport); ok {
			base = native.Clone()
		} else {
			base = http.DefaultTransport
		}
	}
	rc.SetTransport(newObservedTransport(base, cfg))
	if cfg.Retry.Enabled {
		rc.SetRetryCount(cfg.Retry.MaxRetries).SetRetryWaitTime(cfg.Retry.WaitTime).SetRetryMaxWaitTime(cfg.Retry.MaxWaitTime)
		rc.AddRetryCondition(func(resp *resty.Response, err error) bool {
			if resp == nil || resp.Request == nil {
				return false
			}
			request := resp.Request
			// Resty leaves DoNotParseResponse bodies open. Retrying would discard
			// a caller-owned stream and leak its transport; only parsed/closed
			// response bodies are eligible for automatic retry.
			if resp.RawResponse != nil {
				if body, ok := resp.RawResponse.Body.(*observedBody); ok && !body.isFinished() {
					return false
				}
			}
			if request.Context().Err() != nil || !canRetry(request) {
				return false
			}
			// Parsing, validation and body-limit errors are not transport errors.
			if errors.Is(err, resty.ErrResponseBodyTooLarge) {
				return false
			}
			var networkErr *url.Error
			return (err != nil && errors.As(err, &networkErr)) || (err == nil && resp.StatusCode() >= 500)
		})
	}
	return &Client{restyClient: rc, cb: setupCircuitBreaker(cfg.CircuitBreaker), cfg: cfg}, nil
}

func canRetry(req *resty.Request) bool {
	safe := req.Method == http.MethodGet || req.Method == http.MethodHead || req.Method == http.MethodOptions
	optIn, _ := req.Context().Value(retryKey{}).(bool)
	if !safe && !optIn {
		return false
	}
	if len(req.FormData) > 0 {
		return false
	}
	// Resty serializes byte/string/struct/map bodies again per attempt. Readers
	// and multipart streams may already be consumed and cannot be replayed safely.
	if _, streaming := req.Body.(io.Reader); streaming {
		return false
	}
	if strings.HasPrefix(req.Header.Get("Content-Type"), "multipart/") {
		return false
	}
	return true
}

// Native exposes trusted Resty overrides. Replacing transport/retry settings can
// bypass wrapper guarantees. Do not mutate Native while requests are in flight.
func (c *Client) Native() *resty.Client { return c.restyClient }

// Execute performs one logical operation. Timeout bounds retries and body reads;
// shorter caller deadlines win. A streamed RawBody must be closed by the caller.
func (c *Client) Execute(ctx context.Context, fn func(*resty.Request) (*resty.Response, error)) (*resty.Response, error) {
	if ctx == nil || fn == nil {
		return nil, errors.New("httpclient: context and request function are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	var settle func(error)
	if c.cb != nil {
		var err error
		settle, err = c.cb.Allow()
		if err != nil {
			cancel()
			return nil, err
		}
	}
	// Ensure a callback panic cannot retain a half-open breaker slot or timer.
	completed := false
	defer func() {
		if !completed {
			cancel()
			if settle != nil {
				settle(errors.New("httpclient: request callback panicked"))
			}
		}
	}()
	resp, err := fn(c.restyClient.R().SetContext(ctx))
	if resp == nil && err == nil {
		err = errors.New("httpclient: request returned no response")
	}
	if settle != nil {
		outcome := err
		if outcome == nil && resp != nil && resp.StatusCode() >= 500 {
			outcome = errServerStatus
		}
		settle(outcome)
	}
	completed = true
	// Resty closes parsed bodies before returning; streamed bodies retain the
	// timeout until EOF/Close. This also cancels timers promptly on early return.
	if resp != nil && resp.RawResponse != nil {
		if body, ok := resp.RawResponse.Body.(*observedBody); ok && err == nil {
			body.afterFinish(cancel)
			return resp, err
		}
	}
	cancel()
	if err != nil {
		return resp, fmt.Errorf("httpclient: request failed: %w", err)
	}
	return resp, nil
}

// CloseIdleConnections releases pooled idle transports without affecting callers.
func (c *Client) CloseIdleConnections() { c.restyClient.GetClient().CloseIdleConnections() }

type quietLogger struct{}

func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Warnf(string, ...any)  {}
func (quietLogger) Debugf(string, ...any) {}
