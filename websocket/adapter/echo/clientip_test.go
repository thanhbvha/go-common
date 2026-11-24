package echo

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestDefaultConnectionKeyIgnoresForwardingHeaders(t *testing.T) {
	handler := NewHandler(Config{})
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.RemoteAddr = "10.0.0.1:4321"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	req.Header.Set("X-Real-IP", "203.0.113.10")
	c := echo.New().NewContext(req, httptest.NewRecorder())

	if got := handler.config.ConnectionKey(c, "user"); got != "10.0.0.1" {
		t.Fatalf("connection key = %q, want direct peer 10.0.0.1", got)
	}
}
