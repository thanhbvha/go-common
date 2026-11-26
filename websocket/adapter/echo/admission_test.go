package echo

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	echoframework "github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
	"strings"
	"time"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func upgradeRequest(remote, forwarded string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.RemoteAddr = remote
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if forwarded != "" {
		req.Header.Set("X-Forwarded-For", forwarded)
	}
	return req
}

func TestUpgradeIsRateLimitedBeforeAuthenticationAndIgnoresForwardedFor(t *testing.T) {
	var authCalls atomic.Int32
	handler := NewHandler(Config{
		Authenticate:      func(echoframework.Context) (string, error) { authCalls.Add(1); return "", errors.New("no") },
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 1, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	engine := echoframework.New()
	engine.GET("/ws", handler.HandleUpgrade)

	var last *httptest.ResponseRecorder
	for _, spoof := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"} {
		last = httptest.NewRecorder()
		engine.ServeHTTP(last, upgradeRequest("192.0.2.50:9", spoof))
	}
	if last.Code != http.StatusTooManyRequests || last.Header().Get("Retry-After") != "1" {
		t.Fatalf("status = %d, Retry-After = %q", last.Code, last.Header().Get("Retry-After"))
	}
	if authCalls.Load() != 1 {
		t.Fatalf("authenticator ran %d times, want 1", authCalls.Load())
	}
}

func TestQuotaRejectionIsHTTP503BeforeUpgrade(t *testing.T) {
	manager := core.NewManagerWithConfig(core.ManagerConfig{MaxConnectionsPerUser: 1, DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	handler := NewHandler(Config{
		Manager:           manager,
		Authenticate:      func(echoframework.Context) (string, error) { return "user", nil },
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	engine := echoframework.New()
	engine.GET("/ws", handler.HandleUpgrade)
	server := httptest.NewServer(engine)
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"

	first, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	deadline := time.Now().Add(2 * time.Second)
	for manager.GetTotalConnections() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	_, response, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil || response == nil {
		t.Fatalf("second dial must be rejected before upgrade, err = %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After = %q", response.StatusCode, response.Header.Get("Retry-After"))
	}
}
