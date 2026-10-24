package ginlog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func TestRequestIDMiddlewarePopulatesLocalAndContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestIDMiddleware())
	router.GET("/", func(c *gin.Context) {
		contextID, ok := ctxkey.GetRequestID(c.Request.Context())
		localValue, localOK := c.Get("request_id")
		localID, stringOK := localValue.(string)
		if !ok || !localOK || !stringOK || contextID == "" || contextID != localID {
			t.Fatalf("context ID = %q, local ID = %q", contextID, localID)
		}
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("response is missing X-Request-ID")
	}
}

func TestRequestIDMiddlewareReplacesInvalidID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestIDMiddleware())
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", strings.Repeat("a", 129))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if got := recorder.Header().Get("X-Request-ID"); got == req.Header.Get("X-Request-ID") || got == "" {
		t.Fatalf("invalid request ID was not replaced: %q", got)
	}
}
