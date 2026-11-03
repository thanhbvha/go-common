//go:build integration

package main

import (
	"context"
	"fmt"
	"github.com/thanhbvha/go-common/v2/auth"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"io"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestProductionSoak exercises the real HTTP/SQL/Redis/auth stack. Fixture
// addresses are supplied only by test-production.sh, never from production.
func TestProductionSoak(t *testing.T) {
	raw := os.Getenv("PRODUCTION_SOAK_DURATION")
	if raw == "" {
		t.Skip("run .github/scripts/test-production.sh")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		t.Fatal("invalid soak duration")
	}
	dsn, redis := os.Getenv("PRODUCTION_TEST_POSTGRES_DSN"), os.Getenv("PRODUCTION_TEST_REDIS_ADDR")
	if dsn == "" || redis == "" {
		t.Fatal("dedicated fixture addresses required")
	}
	svc, url := startReference(t, redis, postgres.New(dsn), 1000)
	token, err := svc.tokens.GenerateToken(auth.UserInfo{ID: "synthetic-soak"}, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConns: 8, MaxIdleConnsPerHost: 8, MaxConnsPerHost: 8}}
	defer client.CloseIdleConnections()
	// Warm connections before recording leak budgets.
	for range 20 {
		if got := call(t, client, url+"/v1/me", token); got != 200 {
			t.Fatalf("warmup %d", got)
		}
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutines := runtime.NumGoroutine()
	var requests atomic.Int64
	var failures atomic.Int64
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					req, _ := http.NewRequestWithContext(ctx, "GET", url+"/v1/me", nil)
					req.Header.Set("Authorization", "Bearer "+token)
					response, err := client.Do(req)
					if err != nil {
						if ctx.Err() == nil {
							failures.Add(1)
						}
						continue
					}
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					requests.Add(1)
					if response.StatusCode != 200 {
						failures.Add(1)
					}
				}
			}
		})
	}
	workers.Wait()
	client.CloseIdleConnections()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("duration=%s requests=%d failures=%d goroutines_before=%d after=%d heap_before=%d after=%d", duration, requests.Load(), failures.Load(), goroutines, runtime.NumGoroutine(), before.HeapAlloc, after.HeapAlloc)
	if failures.Load() != 0 {
		t.Fatalf("%d failed requests", failures.Load())
	}
	if requests.Load() < int64(duration.Seconds()*100) {
		t.Fatal("workload did not sustain 100 requests/second")
	}
	if runtime.NumGoroutine() > goroutines+32 {
		t.Fatal("goroutine budget exceeded")
	}
	if after.HeapAlloc > before.HeapAlloc+(32<<20) {
		t.Fatal(fmt.Sprintf("heap growth budget exceeded: %d", after.HeapAlloc-before.HeapAlloc))
	}
}
