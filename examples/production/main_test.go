package main

import (
	"context"
	"crypto/rand"
	"github.com/alicebob/miniredis/v2"
	"github.com/thanhbvha/go-common/v2/auth"
	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/sqlite"
	"github.com/thanhbvha/go-common/v2/foundation"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func startReference(t *testing.T, redisAddress string, driver backend.Backend, limit int) (*service, string) {
	t.Helper()
	cfg := settings{Address: "127.0.0.1:0", PostgresDSN: "fixture", RedisAddress: redisAddress, AuthSecret: rand.Text() + rand.Text(), RateLimit: limit}
	svc, err := build(cfg, driver, io.Discard, foundation.WithoutSignals())
	if err != nil {
		t.Fatal(err)
	}
	before := httptest.NewRecorder()
	svc.handler.ServeHTTP(before, httptest.NewRequest("GET", "/ready", nil))
	if before.Code != 503 {
		t.Fatalf("startup readiness %d", before.Code)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.app.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("shutdown timeout")
		}
	})
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for svc.app.State() != foundation.StateRunning {
		select {
		case err := <-done:
			t.Fatalf("startup: %v", err)
		case <-deadline.C:
			t.Fatal("startup timeout")
		case <-tick.C:
		}
	}
	return svc, "http://" + svc.server.Addr
}
func call(t *testing.T, client *http.Client, url, token string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), "GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	return res.StatusCode
}
func TestReferenceServiceLifecycleAuthQuotaAndRedisOutage(t *testing.T) {
	redis := miniredis.RunT(t)
	svc, url := startReference(t, redis.Addr(), sqlite.New(":memory:"), 2)
	token, err := svc.tokens.GenerateToken(auth.UserInfo{ID: "synthetic-user"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	for _, tc := range []struct {
		path, token string
		want        int
	}{{"/ready", "", 200}, {"/v1/me", "", 401}, {"/v1/me", token, 200}, {"/v1/me", token, 200}, {"/v1/me", token, 429}} {
		if got := call(t, client, url+tc.path, tc.token); got != tc.want {
			t.Fatalf("%s got %d want %d", tc.path, got, tc.want)
		}
	}
	otherToken, err := svc.tokens.GenerateToken(auth.UserInfo{ID: "another-user"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := call(t, client, url+"/v1/me", otherToken); got != 200 {
		t.Fatalf("independent subject quota got %d", got)
	}
	for _, key := range redis.Keys() {
		if strings.Contains(key, "synthetic-user") || strings.Contains(key, "another-user") {
			t.Fatalf("rate-limit key exposes subject: %q", key)
		}
	}
	redis.Close()
	if got := call(t, client, url+"/v1/me", token); got != 503 {
		t.Fatalf("limiter outage got %d", got)
	}
	if got := call(t, client, url+"/ready", ""); got != 503 {
		t.Fatalf("outage readiness got %d", got)
	}
	if got := call(t, client, url+"/live", ""); got != 200 {
		t.Fatalf("outage liveness got %d", got)
	}
}
func TestReferenceRejectsInvalidSettings(t *testing.T) {
	if _, err := build(settings{AuthSecret: strings.Repeat("x", 32)}, sqlite.New(":memory:"), io.Discard); err == nil {
		t.Fatal("missing settings accepted")
	}
}
