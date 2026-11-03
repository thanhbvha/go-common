package httpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCheckedConfig(t *testing.T) {
	for _, cfg := range []Config{{Timeout: -1}, {BaseURL: "ftp://example.com"}, {Retry: RetryConfig{MaxRetries: -1}}, {CircuitBreaker: CBConfig{Enabled: true, ReadyToTripFailRatio: 2}}} {
		if _, err := NewClient(cfg); err == nil {
			t.Fatalf("accepted config %+v", cfg)
		}
	}
}

func TestBaseURLRequiresPureOriginOrBasePath(t *testing.T) {
	for _, base := range []string{
		"https://example.test/api?token=secret",
		"https://example.test/api?",
		"https://example.test/api#fragment",
		"https://example.test/api#",
		"https://user:password@example.test/api",
		"https:opaque",
	} {
		t.Run(base, func(t *testing.T) {
			if _, err := NewClient(DefaultConfig(base)); err == nil {
				t.Fatalf("accepted non-pure base URL %q", base)
			}
		})
	}
	for _, base := range []string{"https://example.test", "http://example.test", "https://example.test/api/v1/"} {
		t.Run(base, func(t *testing.T) {
			cfg := DefaultConfig(base)
			cfg.Retry.Enabled, cfg.CircuitBreaker.Enabled = false, false
			var target string
			cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				target = r.URL.String()
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: http.NoBody, Request: r}, nil
			})
			client, err := NewClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			if _, err = client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) { return r.Get("/resource") }); err != nil {
				t.Fatal(err)
			}
			want := strings.TrimRight(base, "/") + "/resource"
			if target != want {
				t.Fatalf("target=%q want=%q", target, want)
			}
		})
	}
}

func TestRetryRequiresSafeMethodAndReplayableBody(t *testing.T) {
	for _, tc := range []struct {
		method string
		body   any
		optIn  bool
		want   int
	}{
		{http.MethodGet, nil, false, 3}, {http.MethodPost, "payload", false, 1},
		{http.MethodPost, "payload", true, 3}, {http.MethodPost, strings.NewReader("stream"), true, 1},
	} {
		calls := 0
		cfg := DefaultConfig("http://example.test")
		cfg.Retry.MaxRetries, cfg.Retry.WaitTime, cfg.Retry.MaxWaitTime = 2, time.Millisecond, time.Millisecond
		cfg.CircuitBreaker.Enabled = false
		cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("unavailable")), Request: r}, nil
		})
		client, err := NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if tc.optIn {
			ctx = WithIdempotentRetry(ctx)
		}
		_, err = client.Execute(ctx, func(r *resty.Request) (*resty.Response, error) {
			if tc.body != nil {
				r.SetBody(tc.body)
			}
			return r.Execute(tc.method, "/")
		})
		if err != nil || calls != tc.want {
			t.Fatalf("%s optIn=%v calls=%d want=%d err=%v", tc.method, tc.optIn, calls, tc.want, err)
		}
	}
}

func TestTransportSpansEndOnNetworkErrorAndDTOResponse(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	cfg := DefaultConfig("http://example.test")
	cfg.Retry.Enabled, cfg.CircuitBreaker.Enabled = false, false
	cfg.TracerProvider = provider
	cfg.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("sensitive-network-detail") })
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) { return r.Get("/?token=secret") })
	if len(recorder.Ended()) != 1 {
		t.Fatalf("network spans: started=%d ended=%d", len(recorder.Started()), len(recorder.Ended()))
	}
	cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"message":"invalid"}`)), Request: r}, nil
	})
	client, err = NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) {
		return r.SetError(&struct{ Message string }{}).Get("/?token=secret")
	})
	if err != nil || len(recorder.Ended()) != 2 {
		t.Fatalf("DTO request=%v spans=%d", err, len(recorder.Ended()))
	}
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if strings.Contains(attr.Value.Emit(), "secret") {
				t.Fatal("sensitive span attribute", attr)
			}
		}
		for _, event := range span.Events() {
			for _, attr := range event.Attributes {
				if strings.Contains(attr.Value.Emit(), "sensitive-network") {
					t.Fatal("sensitive event", attr)
				}
			}
		}
	}
}

func TestRetryBudgetUsesOperationDeadline(t *testing.T) {
	cfg := DefaultConfig("http://example.test")
	cfg.Timeout = 20 * time.Millisecond
	cfg.Retry.WaitTime = time.Second
	cfg.Retry.MaxWaitTime = time.Second
	cfg.CircuitBreaker.Enabled = false
	calls := 0
	cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("unavailable")), Request: r}, nil
	})
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) { return r.Get("/") })
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || time.Since(started) > time.Second {
		t.Fatalf("calls=%d duration=%v err=%v", calls, time.Since(started), err)
	}
}

func TestStreamingResponseRetainsContextUntilClose(t *testing.T) {
	cfg := DefaultConfig("http://example.test")
	cfg.CircuitBreaker.Enabled = false
	cfg.Retry.Enabled = false
	var transportContext context.Context
	cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		transportContext = r.Context()
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("body")), Request: r}, nil
	})
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) { return r.SetDoNotParseResponse(true).Get("/") })
	if err != nil {
		t.Fatal(err)
	}
	if transportContext.Err() != nil {
		t.Fatal("stream canceled at Execute return")
	}
	if err := resp.RawBody().Close(); err != nil {
		t.Fatal(err)
	}
	if transportContext.Err() == nil {
		t.Fatal("stream context retained after close")
	}
}

func TestStreamingServerErrorDoesNotRetryOrLeakBodies(t *testing.T) {
	cfg := DefaultConfig("http://example.test")
	cfg.Retry.WaitTime, cfg.Retry.MaxWaitTime = time.Millisecond, time.Millisecond
	cfg.CircuitBreaker.Enabled = false
	calls, closed := 0, 0
	cfg.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 503, Header: http.Header{}, Body: &closeObserver{Reader: strings.NewReader("stream"), close: func() { closed++ }}, Request: r}, nil
	})
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Execute(context.Background(), func(r *resty.Request) (*resty.Response, error) { return r.SetDoNotParseResponse(true).Get("/") })
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("raw streaming response retried %d times", calls)
	}
	if err := resp.RawBody().Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("closed %d bodies, expected one", closed)
	}
}

type closeObserver struct {
	io.Reader
	close func()
}

func (b *closeObserver) Close() error { b.close(); return nil }
