// Package nethttpauth provides authentication middleware for the standard
// library's net/http package.
package nethttpauth

import (
	"net/http"

	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/auth/middleware/internal/authhttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	httpresp "github.com/thanhbvha/go-common/web/response/nethttp"
)

const (
	missingTokenMessage = "Missing or invalid Authorization header"
	invalidTokenMessage = "Invalid or expired token"
)

// Middleware protects a net/http handler using standard JWT authentication.
func Middleware(manager *auth.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := authhttp.BearerToken(r.Header.Get("Authorization"))
			if tokenString == "" {
				_ = httpresp.Error(w, r, http.StatusUnauthorized, missingTokenMessage)
				return
			}

			userInfo, err := manager.ExtractUserInfo(tokenString)
			if err != nil {
				_ = httpresp.Error(w, r, http.StatusUnauthorized, invalidTokenMessage)
				return
			}

			ctx := ctxkey.SetUserInfo(r.Context(), userInfo)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AADExtractor extracts dynamic Additional Authenticated Data from a request,
// such as an HttpOnly session cookie or a device header.
type AADExtractor func(*http.Request) []byte

// EncryptedMiddleware protects a net/http handler using encrypted JWT
// authentication and optional dynamic AAD token binding.
func EncryptedMiddleware(manager *auth.EncryptedManager, aadExtractor AADExtractor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := authhttp.BearerToken(r.Header.Get("Authorization"))
			if tokenString == "" {
				_ = httpresp.Error(w, r, http.StatusUnauthorized, missingTokenMessage)
				return
			}

			var aad []byte
			if aadExtractor != nil {
				aad = aadExtractor(r)
			}
			userInfo, err := manager.ValidateToken(tokenString, aad)
			if err != nil {
				_ = httpresp.Error(w, r, http.StatusUnauthorized, invalidTokenMessage)
				return
			}

			ctx := ctxkey.SetUserInfo(r.Context(), userInfo)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
