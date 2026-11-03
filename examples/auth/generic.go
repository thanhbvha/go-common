package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/auth"
	nethttpauth "github.com/thanhbvha/go-common/v2/auth/middleware/nethttp"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	nethttplimit "github.com/thanhbvha/go-common/v2/ratelimit/middleware/nethttp"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
)

const (
	accessTokenLifetime  = 15 * time.Minute
	refreshTokenLifetime = 7 * 24 * time.Hour
)

type exampleClaims struct {
	UserID    string `json:"user_id"`
	Role      string `json:"role"`
	SessionID string `json:"sid"`
	jwt.RegisteredClaims
}
type refreshRequest struct {
	SessionID    string `json:"session_id"`
	RefreshToken string `json:"refresh_token"`
}

func runGenericDemo() error {
	secret, err := requiredEnv("AUTH_JWT_SECRET")
	if err != nil {
		return err
	}
	addr := os.Getenv("AUTH_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("AUTH_REDIS_PASSWORD")})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = client.Ping(ctx).Err()
	cancel()
	if err != nil {
		return fmt.Errorf("connect to Redis: %w", err)
	}
	sessions, err := auth.NewRedisSessionStore(auth.SessionStoreOptions{Redis: client, Namespace: "example:auth"})
	if err != nil {
		return err
	}
	manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*exampleClaims]{SecretKey: secret, Revocation: sessions, Issuer: exampleIssuer, Audience: []string{exampleAudience}, Leeway: 30 * time.Second, MaxLifetime: accessTokenLifetime, TokenID: func(c *exampleClaims) (string, error) { return c.SessionID, nil }})
	if err != nil {
		return err
	}
	validator, err := auth.NewGenericTokenValidator(manager, func() *exampleClaims { return &exampleClaims{} }, nil)
	if err != nil {
		return err
	}
	limiter, err := ratelimit.NewRedisLimiter(client, "example:auth:rate")
	if err != nil {
		return err
	}
	limit, err := nethttplimit.Middleware(limiter, ratelimit.MiddlewareConfig{Policy: ratelimit.Config{Limit: 10, Window: time.Minute}, FailurePolicy: ratelimit.FailClosed}, nil)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	// Demonstration credentials only: replace this fixed identity with verified
	// credentials and a server-side role lookup in the application's identity store.
	mux.Handle("POST /login", limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, err := sessions.Create(r.Context(), "generic-user", refreshTokenLifetime)
		if err != nil {
			http.Error(w, "Session service unavailable", 503)
			return
		}
		token, _, err := issueGenericAccessToken(r.Context(), manager, session.Subject, "admin", session.ID)
		if err != nil {
			_ = sessions.Revoke(r.Context(), session.ID)
			http.Error(w, "Token issuance failed", 500)
			return
		}
		writeJSON(w, 200, map[string]any{"access_token": token, "refresh_token": session.RefreshToken, "session_id": session.ID, "token_type": "Bearer", "expires_in": int(accessTokenLifetime.Seconds())})
	})))
	mux.Handle("POST /refresh", limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request refreshRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request.SessionID == "" || request.RefreshToken == "" {
			http.Error(w, "Invalid refresh request", 400)
			return
		}
		session, err := sessions.Rotate(r.Context(), request.SessionID, request.RefreshToken)
		if err != nil {
			status := 503
			if errors.Is(err, auth.ErrInvalidRefreshToken) || errors.Is(err, auth.ErrRefreshReplay) || errors.Is(err, auth.ErrSessionExhausted) {
				status = 401
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		token, _, err := issueGenericAccessToken(r.Context(), manager, session.Subject, "admin", session.ID)
		if err != nil {
			http.Error(w, "Token issuance failed; sign in again", 500)
			return
		}
		writeJSON(w, 200, map[string]any{"access_token": token, "refresh_token": session.RefreshToken, "session_id": session.ID, "token_type": "Bearer", "expires_in": int(accessTokenLifetime.Seconds())})
	})))
	protected := nethttpauth.GenericMiddleware(validator)
	mux.Handle("GET /api/profile", protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := genericClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "Unauthorized", 401)
			return
		}
		writeJSON(w, 200, map[string]any{"claims": claims, "stack": "net/http"})
	})))
	mux.Handle("POST /logout", protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := genericClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "Unauthorized", 401)
			return
		}
		if err := sessions.Revoke(r.Context(), claims.SessionID); err != nil {
			http.Error(w, "Session service unavailable", 503)
			return
		}
		w.WriteHeader(204)
	})))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { setNoStore(w.Header()); mux.ServeHTTP(w, r) })
	runHTTPServer("generic Redis session net/http", &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second})
	return nil
}

func issueGenericAccessToken(ctx context.Context, manager auth.ContextGenericManager[*exampleClaims], userID, role, sessionID string) (string, *exampleClaims, error) {
	now := time.Now()
	id, err := randomID()
	if err != nil {
		return "", nil, err
	}
	claims := &exampleClaims{UserID: userID, Role: role, SessionID: sessionID, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(accessTokenLifetime)), IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), Issuer: exampleIssuer, Audience: jwt.ClaimStrings{exampleAudience}, Subject: userID, ID: id}}
	token, err := manager.GenerateTokenContext(ctx, claims)
	return token, claims, err
}
func genericClaimsFromContext(ctx context.Context) (*exampleClaims, bool) {
	value, ok := ctxkey.GetAuthClaims(ctx)
	if !ok {
		return nil, false
	}
	claims, ok := value.(*exampleClaims)
	return claims, ok && claims != nil
}
