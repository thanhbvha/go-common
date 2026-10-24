package echomw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestErrorHandlerAcceptsNonStringHTTPErrorMessage(t *testing.T) {
	const secret = "database password is secret"
	server := echo.New()
	server.HTTPErrorHandler = ErrorHandler
	server.GET("/error", func(echo.Context) error {
		return echo.NewHTTPError(http.StatusInternalServerError, map[string]string{"detail": secret})
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
