package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sony/gobreaker/v2"
	"github.com/thanhbvha/go-common/httpclient"
)

func main() {
	fmt.Println("=== HTTPClient Module Examples ===")

	// Uncomment the example you want to run:
	RunRetryExample()
	// RunCircuitBreakerExample()
}

// =====================================================================
// 1. Retry Example
// =====================================================================
func RunRetryExample() {
	fmt.Println("\n--- 1. Retry Example (Flaky Server) ---")

	// Setup a Mock Server that fails the first 3 times, then succeeds.
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		fmt.Printf("[Mock Server] Received request #%d\n", requestCount)

		if requestCount <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "data": "Hello World"}`))
	}))
	defer ts.Close()

	// Configure our resilient HTTP Client
	cfg := httpclient.DefaultConfig(ts.URL)

	// We configure retry to attempt up to 5 times.
	// Since the mock server fails 3 times, the 4th attempt should succeed automatically!
	cfg.Retry.MaxRetries = 5
	cfg.Retry.WaitTime = 500 * time.Millisecond

	client := httpclient.NewClient(cfg)

	fmt.Println("Sending Request (Watch the automatic retries)...")

	// Execute the request
	resp, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		return req.SetHeader("Accept", "application/json").Get("/api/v1/data")
	})

	if err != nil {
		fmt.Printf("Request totally failed: %v\n", err)
		return
	}

	fmt.Printf("\n--- Final Result ---\n")
	fmt.Printf("Status Code: %d\n", resp.StatusCode())
	fmt.Printf("Response Body: %s\n", resp.String())
}

// =====================================================================
// 2. Circuit Breaker Example
// =====================================================================
func RunCircuitBreakerExample() {
	fmt.Println("\n--- 2. Circuit Breaker Example (Dead Server) ---")

	// Setup a Mock Server that ALWAYS fails.
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		fmt.Printf("[Mock Server] Received request #%d\n", requestCount)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer ts.Close()

	// Configure the HTTP Client
	cfg := httpclient.DefaultConfig(ts.URL)
	
	// Disable Retry to purely demonstrate the Circuit Breaker
	cfg.Retry.Enabled = false 
	
	// Configure Circuit Breaker
	cfg.CircuitBreaker.ReadyToTripMinRequests = 3 // Check rules after 3 requests
	cfg.CircuitBreaker.ReadyToTripFailRatio = 0.5 // Trip if >= 50% failed
	cfg.CircuitBreaker.Timeout = 5 * time.Second  // Stay OPEN for 5s before Half-Open

	// Add a callback to watch the Circuit Breaker change state
	cfg.CircuitBreaker.OnStateChange = func(name string, from gobreaker.State, to gobreaker.State) {
		fmt.Printf("\n🚨 [CIRCUIT BREAKER] State changed from %s to %s 🚨\n\n", from.String(), to.String())
	}

	client := httpclient.NewClient(cfg)

	// Fire 6 requests in a loop
	for i := 1; i <= 6; i++ {
		fmt.Printf("Sending Request %d...\n", i)
		_, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
			return req.Get("/api/v1/data")
		})

		if err != nil {
			// Once the breaker trips (Open), it returns an error IMMEDIATELY without hitting the Mock Server!
			fmt.Printf("❌ Request %d failed: %v\n", i, err)
		} else {
			fmt.Printf("✅ Request %d succeeded\n", i)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
