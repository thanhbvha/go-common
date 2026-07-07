package httpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sony/gobreaker/v2"
)

func TestHttpClient_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer ts.Close()

	cfg := DefaultConfig(ts.URL)
	client := NewClient(cfg)

	resp, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		return req.Get("/")
	})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if resp.String() != "OK" {
		t.Errorf("Expected 'OK', got %s", resp.String())
	}
}

func TestHttpClient_CircuitBreaker_Open(t *testing.T) {
	// Create a server that always returns 500
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cfg := DefaultConfig(ts.URL)
	cfg.Retry.Enabled = false // Disable retry to test CB directly
	cfg.CircuitBreaker.ReadyToTripMinRequests = 3
	cfg.CircuitBreaker.ReadyToTripFailRatio = 1.0

	client := NewClient(cfg)

	// Send 3 requests, all will fail (500), which should trip the breaker (MinRequests=3)
	for i := 0; i < 3; i++ {
		_, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
			return req.Get("/")
		})
		if err == nil {
			t.Errorf("Expected error on request %d, got nil", i+1)
		}
	}

	// The 4th request should be blocked by the Circuit Breaker
	_, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		return req.Get("/")
	})

	if err != gobreaker.ErrOpenState {
		t.Errorf("Expected gobreaker.ErrOpenState, got %v", err)
	}
}

func TestHttpClient_Retry(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Finally OK"))
	}))
	defer ts.Close()

	cfg := DefaultConfig(ts.URL)
	cfg.CircuitBreaker.Enabled = false // Disable CB to test Retry
	cfg.Retry.MaxRetries = 3
	cfg.Retry.WaitTime = 10 * time.Millisecond

	client := NewClient(cfg)

	resp, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		return req.Get("/")
	})

	if err != nil {
		t.Fatalf("Expected no error due to retry, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
	if resp.String() != "Finally OK" {
		t.Errorf("Expected 'Finally OK', got %s", resp.String())
	}
}
