package echolog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func TestRequestIDMiddlewarePopulatesLocalAndContext(t *testing.T) {
	server := echo.New()
	server.Use(RequestIDMiddleware())
	server.GET("/", func(c echo.Context) error {
		contextID, ok := ctxkey.GetRequestID(c.Request().Context())
		localID, localOK := c.Get("request_id").(string)
		if !ok || !localOK || contextID == "" || contextID != localID {
			t.Fatalf("context ID = %q, local ID = %q", contextID, localID)
		}
		return c.NoContent(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("response is missing X-Request-ID")
	}
}

func TestRequestIDMiddlewareReplacesInvalidID(t *testing.T) {
	server := echo.New()
	server.Use(RequestIDMiddleware())
	server.GET("/", func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", strings.Repeat("a", 129))
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	if got := recorder.Header().Get("X-Request-ID"); got == req.Header.Get("X-Request-ID") || got == "" {
		t.Fatalf("invalid request ID was not replaced: %q", got)
	}
}
