package ginmw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRecoverReturnsSafeResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		requestID = "gin-request"
		secret    = "database password is secret"
	)
	router := gin.New()
	router.Use(Recover())
	router.Use(func(c *gin.Context) {
		c.Set("request_id", requestID)
		c.Next()
	})
	router.GET("/panic", func(*gin.Context) { panic(secret) })

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "INTERNAL_ERROR" || body["request_id"] != requestID {
		t.Fatalf("unexpected response: %#v", body)
	}
	if message, _ := body["message"].(string); strings.Contains(message, secret) {
		t.Fatalf("response exposed panic value: %q", message)
	}
}
