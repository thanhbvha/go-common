package echo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	echoframework "github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

func TestDefaultOriginPolicyRejectsCrossOrigin(t *testing.T) {
	handler := NewHandler(Config{
		Authenticate:      func(echoframework.Context) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: limiter.NewConnectionLimiter(1),
	})
	serverEngine := echoframework.New()
	serverEngine.GET("/ws", handler.HandleUpgrade)
	server := httptest.NewServer(serverEngine)
	defer server.Close()

	header := make(http.Header)
	header.Set("Origin", "https://evil.example")
	conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", header)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("dial error = %v, response = %#v", err, response)
	}
}

func TestGetRequestIDPrefersContext(t *testing.T) {
	serverEngine := echoframework.New()
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req = req.WithContext(ctxkey.SetRequestID(req.Context(), "context-id"))
	c := serverEngine.NewContext(req, httptest.NewRecorder())
	c.Set("request_id", "local-id")
	if got := getRequestID(c); got != "context-id" {
		t.Fatalf("request ID = %q", got)
	}
}
