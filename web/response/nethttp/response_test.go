package nethttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

func requestWithID(requestID string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	return req.WithContext(ctxkey.SetRequestID(req.Context(), requestID))
}

func TestSuccess(t *testing.T) {
	recorder := httptest.NewRecorder()
	if err := Success(recorder, requestWithID("req-1"), map[string]string{"id": "1"}); err != nil {
		t.Fatal(err)
	}

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	var body response.Response
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Code != 0 || body.Message != "Success" || body.RequestID != "req-1" {
		t.Fatalf("response = %#v", body)
	}
}

func TestResponseStatuses(t *testing.T) {
	tests := []struct {
		name       string
		wantStatus int
		write      func(http.ResponseWriter, *http.Request) error
	}{
		{name: "error", wantStatus: http.StatusForbidden, write: func(w http.ResponseWriter, r *http.Request) error {
			return Error(w, r, http.StatusForbidden, "forbidden")
		}},
		{name: "validation", wantStatus: http.StatusBadRequest, write: func(w http.ResponseWriter, r *http.Request) error {
			return ValidationError(w, r, map[string]string{"name": "required"})
		}},
		{name: "created", wantStatus: http.StatusCreated, write: func(w http.ResponseWriter, r *http.Request) error {
			return Created(w, r, struct{}{})
		}},
		{name: "paginated", wantStatus: http.StatusOK, write: func(w http.ResponseWriter, r *http.Request) error {
			return Paginated(w, r, []int{1}, 1, 1, 1, 10)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			if err := tt.write(recorder, requestWithID("req-2")); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
		})
	}
}
