package fiber

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

func TestUpgradeIsRateLimitedBeforeAuthenticationAndIgnoresForwardedFor(t *testing.T) {
	var authCalls atomic.Int32
	handler := NewHandler(Config{
		Authenticate:      func(*fiberframework.Ctx) (string, error) { authCalls.Add(1); return "", errors.New("no") },
		IPRateLimiter:     limiter.NewKeyedRateLimiter(0.001, 1, 0, 0),
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	app := fiberframework.New()
	app.Get("/ws", handler.HandleUpgrade)

	var last *http.Response
	for _, spoof := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"} {
		req := websocketUpgradeRequest("http://example.com/ws")
		req.Header.Set("X-Forwarded-For", spoof)
		response, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		if last != nil {
			_ = last.Body.Close()
		}
		last = response
	}
	defer last.Body.Close()
	if last.StatusCode != http.StatusTooManyRequests || last.Header.Get("Retry-After") != "1" {
		t.Fatalf("status = %d, Retry-After = %q", last.StatusCode, last.Header.Get("Retry-After"))
	}
	if authCalls.Load() != 1 {
		t.Fatalf("authenticator ran %d times, want 1", authCalls.Load())
	}
}
