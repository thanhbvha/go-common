package gin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	ginframework "github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func probe(handler ginframework.HandlerFunc) int {
	ginframework.SetMode(ginframework.TestMode)
	router := ginframework.New()
	router.GET("/", handler)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
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
	do := func(token, header string) int {
		h := NewHandler(Config{Manager: manager, AdminToken: token})
		ginframework.SetMode(ginframework.TestMode)
		router := ginframework.New()
		router.GET("/stats", h.HandleStats)
		router.GET("/shard", h.HandleShardManagement)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Header.Set("Authorization", header)
		router.ServeHTTP(rec, req)
		first := rec.Code
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/shard?shard=x", nil)
		req.Header.Set("Authorization", header)
		router.ServeHTTP(rec, req)
		if first == http.StatusUnauthorized && rec.Code != http.StatusUnauthorized {
			t.Fatalf("shard endpoint not guarded: %d", rec.Code)
		}
		return first
	}
	if got := do("", "Bearer "); got != http.StatusUnauthorized {
		t.Fatalf("no token configured: %d", got)
	}
	if got := do("s3cret", "Bearer wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", got)
	}
	if got := do("s3cret", "Bearer s3cret"); got != http.StatusOK {
		t.Fatalf("valid token: %d", got)
	}
}
