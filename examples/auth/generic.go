package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"

	"github.com/thanhbvha/go-common/auth"
	nethttpauth "github.com/thanhbvha/go-common/auth/middleware/nethttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

const (
	accessTokenLifetime  = 15 * time.Minute
	refreshTokenLifetime = 7 * 24 * time.Hour
)

type exampleClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type refreshRequest struct {
	UserID       string `json:"user_id"`
	RefreshToken string `json:"refresh_token"`
}

func runGenericDemo() error {
	secret, err := requiredEnv("AUTH_JWT_SECRET")
	if err != nil {
		return err
	}
	redisAddress := os.Getenv("AUTH_REDIS_ADDR")
	if redisAddress == "" {
		redisAddress = "127.0.0.1:6379"
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress, Password: os.Getenv("AUTH_REDIS_PASSWORD"),
	})
	defer func() { _ = redisClient.Close() }()
	pingContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	pingErr := redisClient.Ping(pingContext).Err()
	cancel()
	if pingErr != nil {
		return fmt.Errorf("connect to Redis at %s: %w", redisAddress, pingErr)
	}

	manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*exampleClaims]{
		SecretKey:      secret,
		Redis:          redisClient,
		Issuer:         exampleIssuer,
		Audience:       []string{exampleAudience},
		Leeway:         30 * time.Second,
		MaxLifetime:    accessTokenLifetime,
		MaxTokenSize:   auth.DefaultMaxTokenSize,
		MaxPayloadSize: auth.DefaultMaxPayloadSize,
		TokenID: func(claims *exampleClaims) (string, error) {
			return claims.ID, nil
		},
	})
	if err != nil {
		return fmt.Errorf("configure generic manager: %w", err)
	}
	validator, err := auth.NewGenericTokenValidator(
		manager,
		func() *exampleClaims { return &exampleClaims{} },
		nil, // The manager already performs fail-closed revocation checks.
	)
	if err != nil {
		return fmt.Errorf("configure generic validator: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		accessToken, claims, err := issueGenericAccessToken(r.Context(), manager, "generic-user", "admin")
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		refreshToken, err := manager.GenerateRefreshToken(r.Context(), claims.UserID, refreshTokenLifetime)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		setNoStore(w.Header())
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": accessToken, "refresh_token": refreshToken,
			"token_type": "Bearer", "expires_in": int(accessTokenLifetime.Seconds()),
		})
	})

	mux.HandleFunc("POST /refresh", func(w http.ResponseWriter, r *http.Request) {
		var request refreshRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request.UserID == "" || request.RefreshToken == "" {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		valid, err := manager.VerifyRefreshToken(r.Context(), request.UserID, request.RefreshToken)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		if !valid {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		accessToken, _, err := issueGenericAccessToken(r.Context(), manager, request.UserID, "admin")
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		rotatedRefreshToken, err := manager.GenerateRefreshToken(r.Context(), request.UserID, refreshTokenLifetime)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		setNoStore(w.Header())
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": accessToken, "token_type": "Bearer",
			"refresh_token": rotatedRefreshToken,
			"expires_in":    int(accessTokenLifetime.Seconds()),
		})
	})

	protected := nethttpauth.GenericMiddleware(validator)
	mux.Handle("GET /api/profile", protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := genericClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"claims": claims, "stack": "net/http"})
	})))
	mux.Handle("POST /logout", protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := genericClaimsFromContext(r.Context())
		if !ok || claims.ExpiresAt == nil {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		remaining := time.Until(claims.ExpiresAt.Time)
		if remaining <= 0 {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		if err := manager.RevokeRefreshToken(r.Context(), claims.UserID); err != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		if err := manager.RevokeToken(r.Context(), claims.ID, remaining); err != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})))

	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	runHTTPServer("generic Redis-backed net/http", server)
	return nil
}

func issueGenericAccessToken(ctx context.Context, manager auth.ContextGenericManager[*exampleClaims], userID, role string) (string, *exampleClaims, error) {
	now := time.Now()
	tokenID, err := randomID()
	if err != nil {
		return "", nil, err
	}
	claims := &exampleClaims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(accessTokenLifetime)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    exampleIssuer,
			Audience:  jwt.ClaimStrings{exampleAudience},
			Subject:   userID,
			ID:        tokenID,
		},
	}
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
