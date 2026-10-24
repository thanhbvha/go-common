package nethttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thanhbvha/go-common/health"
)

func TestHandlers(t *testing.T) {
	checker := health.New().WithVersion("v1")
	checker.AddReadinessCheck("database", func(context.Context) error {
		return errors.New("unavailable")
	})
	checker.AddLivenessCheck("process", func(context.Context) error { return nil })

	tests := []struct {
		name        string
		handler     http.Handler
		wantStatus  int
		wantOverall health.Status
	}{
		{name: "health", handler: Handler(checker), wantStatus: http.StatusServiceUnavailable, wantOverall: health.StatusDown},
		{name: "ready", handler: ReadyHandler(checker), wantStatus: http.StatusServiceUnavailable, wantOverall: health.StatusDown},
		{name: "live", handler: LiveHandler(checker), wantStatus: http.StatusOK, wantOverall: health.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			tt.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if got := recorder.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q", got)
			}
			var body health.HealthResponse
			if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Status != tt.wantOverall || body.Version != "v1" {
				t.Fatalf("response = %#v", body)
			}
		})
	}
}

func TestHandlerPassesRequestContext(t *testing.T) {
	type key struct{}
	const value = "request-value"
	checker := health.New().AddReadinessCheck("context", func(ctx context.Context) error {
		if ctx.Value(key{}) != value {
			return errors.New("request context not propagated")
		}
		return nil
	})
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	req = req.WithContext(context.WithValue(req.Context(), key{}, value))
	recorder := httptest.NewRecorder()

	ReadyHandler(checker).ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}
