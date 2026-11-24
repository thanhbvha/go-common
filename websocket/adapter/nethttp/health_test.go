package nethttp

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestHandleHealthCheckStaysAnAliasOfReady(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	handler := NewHandler(Config{Manager: manager})
	_ = manager.Shutdown(context.Background())
	if got := probe(handler.HandleHealthCheck); got != http.StatusServiceUnavailable {
		t.Fatalf("legacy health route = %d, want readiness semantics (503)", got)
	}
}
