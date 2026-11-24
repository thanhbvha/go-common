package gin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	ginframework "github.com/gin-gonic/gin"
)

func TestDefaultConnectionKeyIgnoresForwardingHeaders(t *testing.T) {
	ginframework.SetMode(ginframework.TestMode)
	handler := NewHandler(Config{})
	c, _ := ginframework.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.RemoteAddr = "10.0.0.1:4321"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	req.Header.Set("X-Real-IP", "203.0.113.10")
	c.Request = req

	if got := handler.config.ConnectionKey(c, "user"); got != "10.0.0.1" {
		t.Fatalf("connection key = %q, want direct peer 10.0.0.1", got)
	}
}
