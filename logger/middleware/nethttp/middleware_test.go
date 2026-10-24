package nethttplog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func TestRequestIDMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		requestID  string
		wantHeader string
	}{
		{name: "preserves incoming ID", requestID: "client-id", wantHeader: "client-id"},
		{name: "generates ID"},
		{name: "replaces invalid ID", requestID: "invalid id"},
		{name: "replaces oversized ID", requestID: strings.Repeat("a", 129)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var contextID string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				contextID, _ = ctxkey.GetRequestID(r.Context())
				w.WriteHeader(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.requestID != "" {
				req.Header.Set(requestIDHeader, tt.requestID)
			}
			recorder := httptest.NewRecorder()

			RequestIDMiddleware(next).ServeHTTP(recorder, req)

			responseID := recorder.Header().Get(requestIDHeader)
			if responseID == "" || responseID != contextID {
				t.Fatalf("response ID = %q, context ID = %q", responseID, contextID)
			}
			if tt.wantHeader != "" && responseID != tt.wantHeader {
				t.Fatalf("response ID = %q, want %q", responseID, tt.wantHeader)
			}
		})
	}
}

func TestMiddlewarePreservesResponse(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	})
	recorder := httptest.NewRecorder()

	Middleware(next).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/items", nil))

	if recorder.Code != http.StatusCreated || recorder.Body.String() != "created" {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
}
