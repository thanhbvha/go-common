package nethttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func probe(handler http.HandlerFunc) int {
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	return recorder.Code
}

func TestProbesSeparateLivenessFromReadiness(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	handler := NewHandler(Config{Manager: manager})

	if live, ready := probe(handler.HandleLive), probe(handler.HandleReady); live != http.StatusOK || ready != http.StatusOK {
		t.Fatalf("running node: live = %d, ready = %d", live, ready)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if live := probe(handler.HandleLive); live != http.StatusOK {
		t.Fatalf("a stopped node must stay live so it is not restarted mid-drain, got %d", live)
	}
	if ready := probe(handler.HandleReady); ready != http.StatusServiceUnavailable {
		t.Fatalf("a stopped node must not be ready, got %d", ready)
	}
}

func TestAdminEndpointsRequireToken(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	for token, header := range map[string]string{"": "Bearer ", "s3cret": ""} { // closed by default; missing header
		h := NewHandler(Config{Manager: manager, AdminToken: token})
		for name, fn := range map[string]http.HandlerFunc{"stats": h.HandleStats, "shard": h.HandleShardManagement} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/?shard=x", nil)
			req.Header.Set("Authorization", header)
			fn(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s token=%q: got %d, want 401", name, token, rec.Code)
			}
		}
	}
	h := NewHandler(Config{Manager: manager, AdminToken: "s3cret"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	h.HandleStats(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token: got %d", rec.Code)
	}
}

func TestReadyBodyDoesNotExposeTopology(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	rec := httptest.NewRecorder()
	NewHandler(Config{Manager: manager}).HandleReady(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if strings.Contains(rec.Body.String(), "brokerReady") {
		t.Fatalf("readiness leaks dependency state: %s", rec.Body.String())
	}
}
