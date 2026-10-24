package gin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ginframework "github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

func TestDefaultOriginPolicyRejectsCrossOrigin(t *testing.T) {
	ginframework.SetMode(ginframework.TestMode)
	handler := NewHandler(Config{
		Authenticate:      func(*ginframework.Context) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: limiter.NewConnectionLimiter(1),
	})
	router := ginframework.New()
	router.GET("/ws", handler.HandleUpgrade)
	server := httptest.NewServer(router)
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
	recorder := httptest.NewRecorder()
	c, _ := ginframework.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req = req.WithContext(ctxkey.SetRequestID(req.Context(), "context-id"))
	c.Request = req
	c.Set("request_id", "local-id")
	if got := getRequestID(c); got != "context-id" {
		t.Fatalf("request ID = %q", got)
	}
}
