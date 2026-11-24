package gin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	ginframework "github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
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
	ginframework.SetMode(ginframework.TestMode)
	var authCalls atomic.Int32
	handler := NewHandler(Config{
		Authenticate:      func(*ginframework.Context) (string, error) { authCalls.Add(1); return "", errors.New("no") },
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 1, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	router := ginframework.New()
	router.GET("/ws", handler.HandleUpgrade)

	var last *httptest.ResponseRecorder
	for _, spoof := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"} {
		last = httptest.NewRecorder()
		router.ServeHTTP(last, upgradeRequest("192.0.2.50:9", spoof))
	}
	if last.Code != http.StatusTooManyRequests || last.Header().Get("Retry-After") != "1" {
		t.Fatalf("status = %d, Retry-After = %q", last.Code, last.Header().Get("Retry-After"))
	}
	if authCalls.Load() != 1 {
		t.Fatalf("authenticator ran %d times, want 1", authCalls.Load())
	}
}
