package nethttp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

func TestHandleUpgradeRequiresWebSocket(t *testing.T) {
	authCalled := false
	handler := NewHandler(Config{Authenticate: func(*http.Request) (string, error) {
		authCalled = true
		return "user", nil
	}})
	recorder := httptest.NewRecorder()

	handler.HandleUpgrade(recorder, httptest.NewRequest(http.MethodGet, "/ws", nil))

	if recorder.Code != http.StatusUpgradeRequired || authCalled {
		t.Fatalf("status = %d, authCalled = %v", recorder.Code, authCalled)
	}
}

func TestDefaultAuthenticationRejectsUpgrade(t *testing.T) {
	handler := NewHandler()
	recorder := httptest.NewRecorder()
	req := websocketRequest("/ws?token=unverified")

	handler.HandleUpgrade(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDefaultOriginPolicy(t *testing.T) {
	handler := NewHandler(Config{
		Authenticate:      func(*http.Request) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: limiter.NewConnectionLimiter(2),
	})
	server := httptest.NewServer(http.HandlerFunc(handler.HandleUpgrade))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	header := make(http.Header)
	header.Set("Origin", "https://evil.example")
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, header)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("dial error = %v, response = %#v", err, response)
	}
}

func TestUpgradeConnectsAndReleasesLimit(t *testing.T) {
	connectionLimiter := limiter.NewConnectionLimiter(1)
	handler := NewHandler(Config{
		Authenticate:      func(*http.Request) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: connectionLimiter,
	})
	server := httptest.NewServer(http.HandlerFunc(handler.HandleUpgrade))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if response != nil {
			t.Fatalf("dial error = %v, status = %d", err, response.StatusCode)
		}
		t.Fatal(err)
	}
	waitForConnectionCount(t, connectionLimiter, "127.0.0.1", 1)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForConnectionCount(t, connectionLimiter, "127.0.0.1", 0)
}

func TestConnectionKeyControlsLimiterKey(t *testing.T) {
	connectionLimiter := limiter.NewConnectionLimiter(1)
	handler := NewHandler(Config{
		Authenticate:      func(*http.Request) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: connectionLimiter,
		ConnectionKey:     func(*http.Request, string) string { return "account:user" },
	})
	server := httptest.NewServer(http.HandlerFunc(handler.HandleUpgrade))
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForConnectionCount(t, connectionLimiter, "account:user", 1)
	_ = conn.Close()
	waitForConnectionCount(t, connectionLimiter, "account:user", 0)
}

func TestGetRequestIDPrefersContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("X-Request-ID", "header-id")
	req = req.WithContext(ctxkey.SetRequestID(req.Context(), "context-id"))
	if got := getRequestID(req); got != "context-id" {
		t.Fatalf("request ID = %q", got)
	}
}

func websocketRequest(target string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return req
}

func waitForConnectionCount(t *testing.T, connectionLimiter *limiter.ConnectionLimiter, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := connectionLimiter.GetConnectionCount(key); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("connection count = %d, want %d", connectionLimiter.GetConnectionCount(key), want))
}
