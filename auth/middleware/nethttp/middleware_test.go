package nethttpauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

const (
	testJWTSecret = "jwt-secret-key-that-is-at-least-32-bytes"
	testAESKey    = "12345678901234567890123456789012"
)

func TestMiddleware(t *testing.T) {
	manager, err := auth.NewManager(testJWTSecret)
	if err != nil {
		t.Fatal(err)
	}
	wantUser := auth.UserInfo{ID: "user-1", Role: "admin"}
	token, err := manager.GenerateToken(wantUser, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantCalled bool
	}{
		{name: "valid token", header: "Bearer " + token, wantStatus: http.StatusNoContent, wantCalled: true},
		{name: "missing token", wantStatus: http.StatusUnauthorized},
		{name: "invalid token", header: "Bearer invalid", wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				value, ok := ctxkey.GetUserInfo(r.Context())
				if !ok {
					t.Fatal("user info missing from request context")
				}
				gotUser, ok := value.(*auth.UserInfo)
				if !ok || gotUser.ID != wantUser.ID || gotUser.Role != wantUser.Role {
					t.Fatalf("user info = %#v", value)
				}
				w.WriteHeader(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodGet, "/profile", nil)
			req.Header.Set("Authorization", tt.header)
			req = req.WithContext(ctxkey.SetRequestID(req.Context(), "req-auth"))
			recorder := httptest.NewRecorder()

			Middleware(manager)(next).ServeHTTP(recorder, req)

			if recorder.Code != tt.wantStatus || called != tt.wantCalled {
				t.Fatalf("status = %d, called = %v", recorder.Code, called)
			}
			if !tt.wantCalled {
				var body response.Response
				if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.Code != http.StatusUnauthorized || body.RequestID != "req-auth" {
					t.Fatalf("response = %#v", body)
				}
			}
		})
	}
}

func TestEncryptedMiddlewareUsesAAD(t *testing.T) {
	manager, err := auth.NewEncryptedManager(testJWTSecret, testAESKey)
	if err != nil {
		t.Fatal(err)
	}
	wantUser := auth.UserInfo{ID: "encrypted-user"}
	token, err := manager.GenerateToken(wantUser, time.Hour, []byte("session-1"))
	if err != nil {
		t.Fatal(err)
	}
	aadExtractor := func(r *http.Request) []byte {
		cookie, err := r.Cookie("session_id")
		if err != nil {
			return nil
		}
		return []byte(cookie.Value)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, _ := ctxkey.GetUserInfo(r.Context())
		gotUser, ok := value.(*auth.UserInfo)
		if !ok || gotUser.ID != wantUser.ID {
			t.Fatalf("user info = %#v", value)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	for _, tt := range []struct {
		name       string
		sessionID  string
		wantStatus int
	}{
		{name: "matching AAD", sessionID: "session-1", wantStatus: http.StatusNoContent},
		{name: "wrong AAD", sessionID: "session-2", wantStatus: http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/profile", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			req.AddCookie(&http.Cookie{Name: "session_id", Value: tt.sessionID})
			recorder := httptest.NewRecorder()

			EncryptedMiddleware(manager, aadExtractor)(next).ServeHTTP(recorder, req)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}
