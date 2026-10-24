package echomw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestRecoverReturnsSafeResponse(t *testing.T) {
	const (
		requestID = "echo-request"
		secret    = "database password is secret"
	)
	server := echo.New()
	server.Use(Recover())
	server.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("request_id", requestID)
			return next(c)
		}
	})
	server.GET("/panic", func(echo.Context) error { panic(secret) })

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/panic", nil))
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
