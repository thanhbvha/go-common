package nethttp

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

func TestUpgradeIsRateLimitedBeforeAuthentication(t *testing.T) {
	var authCalls atomic.Int32
	handler := NewHandler(Config{
		Authenticate: func(*http.Request) (string, error) {
			authCalls.Add(1)
			return "", http.ErrNoCookie
		},
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 2, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})

	var last *httptest.ResponseRecorder
	for i := 0; i < 5; i++ {
		last = httptest.NewRecorder()
		req := websocketRequest("/ws")
		req.RemoteAddr = "198.51.100.7:5555"
		handler.HandleUpgrade(last, req)
	}
	if last.Code != http.StatusTooManyRequests || last.Header().Get("Retry-After") != "1" {
		t.Fatalf("status = %d, Retry-After = %q", last.Code, last.Header().Get("Retry-After"))
	}
	if got := authCalls.Load(); got != 2 {
		t.Fatalf("authenticator ran %d times; throttled requests must never reach it (want 2, the burst)", got)
	}
}

func TestRateLimitIsPerSourceNotGlobal(t *testing.T) {
	handler := NewHandler(Config{
		Authenticate:      func(*http.Request) (string, error) { return "", http.ErrNoCookie },
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 1, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	hit := func(addr string) int {
		recorder := httptest.NewRecorder()
		req := websocketRequest("/ws")
		req.RemoteAddr = addr
		handler.HandleUpgrade(recorder, req)
		return recorder.Code
	}
	hit("203.0.113.1:1")
	if got := hit("203.0.113.1:2"); got != http.StatusTooManyRequests {
		t.Fatalf("abusive source status = %d", got)
	}
	if got := hit("203.0.113.2:1"); got != http.StatusUnauthorized {
		t.Fatalf("other source status = %d, want 401 (reached authentication)", got)
	}
}

func TestForwardedHeaderCannotEvadeUpgradeRateLimit(t *testing.T) {
	handler := NewHandler(Config{
		Authenticate:      func(*http.Request) (string, error) { return "", http.ErrNoCookie },
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 1, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	var codes []int
	for _, spoof := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"} {
		recorder := httptest.NewRecorder()
		req := websocketRequest("/ws")
		req.RemoteAddr = "192.0.2.50:9"
		req.Header.Set("X-Forwarded-For", spoof)
		handler.HandleUpgrade(recorder, req)
		codes = append(codes, recorder.Code)
	}
	if codes[1] != http.StatusTooManyRequests || codes[2] != http.StatusTooManyRequests {
		t.Fatalf("spoofed X-Forwarded-For evaded the limit: %v", codes)
	}
}
