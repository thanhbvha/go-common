package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/thanhbvha/go-common/auth"
	nethttpauth "github.com/thanhbvha/go-common/auth/middleware/nethttp"
)

const sessionCookieName = "auth_example_session"

func runEncryptedDemo() error {
	secret, err := requiredEnv("AUTH_JWT_SECRET")
	if err != nil {
		return err
	}
	aesKey, err := requiredEnv("AUTH_AES_KEY")
	if err != nil {
		return err
	}
	manager, err := auth.NewEncryptedManagerWithOptions(auth.EncryptedManagerOptions{
		SecretKey:      secret,
		AESKey:         aesKey,
		Issuer:         exampleIssuer,
		Audience:       []string{exampleAudience},
		Leeway:         30 * time.Second,
		MaxLifetime:    15 * time.Minute,
		MaxTokenSize:   auth.DefaultMaxTokenSize,
		MaxPayloadSize: auth.DefaultMaxPayloadSize,
		AADPolicy:      auth.RequireAAD,
	})
	if err != nil {
		return fmt.Errorf("configure encrypted manager: %w", err)
	}

	aadExtractor := func(r *http.Request) []byte {
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil {
			return nil
		}
		aad, err := encryptedAAD(cookie.Value)
		if err != nil {
			return nil
		}
		return aad
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		sessionID, err := randomID()
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		aad, err := encryptedAAD(sessionID)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		token, err := manager.GenerateTokenContext(r.Context(), auth.UserInfo{
			ID: "encrypted-user", Role: "admin", Email: "encrypted@example.com",
		}, 15*time.Minute, aad)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name: sessionCookieName, Value: sessionID, Path: "/", HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			// Set Secure=true behind HTTPS. It is false only for this localhost demo.
			Secure: false,
		})
		setNoStore(w.Header())
		writeJSON(w, http.StatusOK, map[string]string{
			"access_token": token,
			"token_type":   "Bearer",
		})
	})

	protected := nethttpauth.EncryptedMiddleware(manager, aadExtractor)
	mux.Handle("GET /api/profile", protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := userFromContext(r.Context())
		if !ok {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		claims, ok := registeredClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"user": user, "claims": claims, "encrypted": true, "stack": "net/http",
		})
	})))

	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	runHTTPServer("encrypted net/http", server)
	return nil
}

func encryptedAAD(sessionID string) ([]byte, error) {
	return (auth.AADContext{
		Issuer: exampleIssuer, Audience: exampleAudience, SessionID: sessionID,
	}).Bytes()
}
