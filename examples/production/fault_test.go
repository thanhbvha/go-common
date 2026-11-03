package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/thanhbvha/go-common/v2/auth"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/sqlite"
)

// Repeated outages exercise the existing pools across reconnects, rather than
// replacing the service after a dependency failure.
func TestReferenceRedisRepeatedOutageRecovery(t *testing.T) {
	redis := miniredis.RunT(t)
	svc, url := startReference(t, redis.Addr(), sqlite.New(":memory:"), 1000)
	token, err := svc.tokens.GenerateToken(auth.UserInfo{ID: "fault-fixture"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	for cycle := range 3 {
		if got := call(t, client, url+"/v1/me", token); got != http.StatusOK {
			t.Fatalf("cycle %d before outage: %d", cycle, got)
		}
		redis.Close()
		for _, path := range []string{"/v1/me", "/ready"} {
			if got := call(t, client, url+path, token); got != http.StatusServiceUnavailable {
				t.Fatalf("cycle %d %s during outage: %d", cycle, path, got)
			}
		}
		if got := call(t, client, url+"/live", ""); got != http.StatusOK {
			t.Fatalf("cycle %d liveness: %d", cycle, got)
		}
		if err := redis.Restart(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for call(t, client, url+"/ready", "") != http.StatusOK {
			if time.Now().After(deadline) {
				t.Fatalf("cycle %d readiness did not recover", cycle)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if got := call(t, client, url+"/v1/me", token); got != http.StatusOK {
		t.Fatalf("authenticated request after recovery: %d", got)
	}
}
