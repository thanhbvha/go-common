package nethttpauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

type genericHTTPClaims struct {
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

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
				claimsValue, ok := ctxkey.GetAuthClaims(r.Context())
				claims, valid := claimsValue.(jwt.RegisteredClaims)
				if !ok || !valid || claims.ID == "" || claims.Subject != wantUser.ID {
					t.Fatalf("registered claims = %#v", claimsValue)
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

func TestMiddlewareWithNilManagerFailsClosed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/profile", nil)
	req.Header.Set("Authorization", "Bearer token")
	recorder := httptest.NewRecorder()
	Middleware(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler must not be called")
	})).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
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
		claimsValue, ok := ctxkey.GetAuthClaims(r.Context())
		claims, valid := claimsValue.(jwt.RegisteredClaims)
		if !ok || !valid || claims.ID == "" || claims.Subject != wantUser.ID {
			t.Fatalf("registered claims = %#v", claimsValue)
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

func TestGenericMiddlewareValidatesAndStoresClaims(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*genericHTTPClaims]{
		SecretKey:   testJWTSecret,
		Redis:       client,
		MaxLifetime: time.Hour,
		TokenID:     func(claims *genericHTTPClaims) (string, error) { return claims.ID, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := auth.NewGenericTokenValidator(
		manager,
		func() *genericHTTPClaims { return &genericHTTPClaims{} },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims := &genericHTTPClaims{
		UserID: "generic-user",
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "generic-token",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Subject:   "generic-user",
		},
	}
	token, err := manager.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, ok := ctxkey.GetUserInfo(r.Context())
		got, valid := value.(*genericHTTPClaims)
		if !ok || !valid || got.UserID != claims.UserID {
			t.Fatalf("generic claims = %#v", value)
		}
		claimsValue, ok := ctxkey.GetAuthClaims(r.Context())
		if !ok || claimsValue != got {
			t.Fatalf("generic auth claims = %#v", claimsValue)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/profile", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	GenericMiddleware(validator)(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	if err := manager.RevokeToken(request.Context(), claims.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	GenericMiddleware(validator)(next).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status = %d", recorder.Code)
	}
}
