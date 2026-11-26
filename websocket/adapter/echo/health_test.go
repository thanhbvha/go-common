package echo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	echoframework "github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func probe(handler echoframework.HandlerFunc) int {
	engine := echoframework.New()
	engine.GET("/", handler)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
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
	do := func(token, header, path string) int {
		h := NewHandler(Config{Manager: manager, AdminToken: token})
		engine := echoframework.New()
		engine.GET("/stats", h.HandleStats)
		engine.GET("/shard", h.HandleShardManagement)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", header)
		engine.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, path := range []string{"/stats", "/shard?shard=x"} {
		if got := do("", "Bearer ", path); got != http.StatusUnauthorized {
			t.Fatalf("%s no token configured: %d", path, got)
		}
		if got := do("s3cret", "Bearer wrong", path); got != http.StatusUnauthorized {
			t.Fatalf("%s wrong token: %d", path, got)
		}
	}
	if got := do("s3cret", "Bearer s3cret", "/stats"); got != http.StatusOK {
		t.Fatalf("valid token: %d", got)
	}
}
