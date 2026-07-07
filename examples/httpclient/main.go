package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/thanhbvha/go-common/httpclient"
)

func main() {
	fmt.Println("=== HTTPClient Module Example ===")

	// 1. Setup a Mock Server that occasionally fails (Simulating a flaky 3rd-party API)
	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		fmt.Printf("[Mock Server] Received request #%d\n", requestCount)
		
		if requestCount <= 3 {
			// Fail the first 3 times
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
			return
		}
		
		// Succeed on the 4th time
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "success", "data": "Hello World"}`))
	}))
	defer ts.Close()

	// 2. Configure our resilient HTTP Client
	cfg := httpclient.DefaultConfig(ts.URL)
	
	// We configure retry to attempt up to 5 times.
	// Since the mock server fails 3 times, the 4th attempt should succeed automatically!
	cfg.Retry.MaxRetries = 5
	cfg.Retry.WaitTime = 500 * time.Millisecond
	
	client := httpclient.NewClient(cfg)

	fmt.Println("\n--- Sending Request (Watch the automatic retries) ---")
	
	// 3. Execute the request
	resp, err := client.Execute(context.Background(), func(req *resty.Request) (*resty.Response, error) {
		// Set headers, query params, body, etc. using Resty's elegant chainable API
		return req.
			SetHeader("Accept", "application/json").
			Get("/api/v1/data")
	})

	if err != nil {
		fmt.Printf("Request totally failed: %v\n", err)
		return
	}

	fmt.Printf("\n--- Final Result ---\n")
	fmt.Printf("Status Code: %d\n", resp.StatusCode())
	fmt.Printf("Response Body: %s\n", resp.String())
}
