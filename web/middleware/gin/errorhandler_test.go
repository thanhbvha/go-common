package ginmw

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestErrorHandlerMasksUnknownError(t *testing.T) {
	const secret = "database password is secret"
	gin.SetMode(gin.TestMode)
	server := gin.New()
	server.Use(ErrorHandler())
	server.GET("/error", func(c *gin.Context) {
		_ = c.Error(errors.New(secret))
	})

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/error", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	message, _ := body["message"].(string)
	if strings.Contains(message, secret) {
		t.Fatalf("response exposed internal error detail: %q", message)
	}
}
