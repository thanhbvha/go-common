package fiber

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
	"net"
	"time"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/v2/websocket/core"
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

func TestQuotaRejectionIsHTTP503BeforeUpgrade(t *testing.T) {
	manager := core.NewManagerWithConfig(core.ManagerConfig{MaxConnectionsPerUser: 1, DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	handler := NewHandler(Config{
		Manager:           manager,
		Authenticate:      func(*fiberframework.Ctx) (string, error) { return "user", nil },
		ConnectionLimiter: limiter.NewConnectionLimiter(10),
	})
	app := fiberframework.New(fiberframework.Config{DisableStartupMessage: true})
	app.Get("/ws", handler.HandleUpgrade)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(listener) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	url := "ws://" + listener.Addr().String() + "/ws"

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
