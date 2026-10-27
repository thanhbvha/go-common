// Package echoauth provides JWT authentication middleware for Echo.
package echoauth

import (
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/auth/middleware/internal/authhttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	echoresp "github.com/thanhbvha/go-common/web/response/echo"
)

// Middleware creates an Echo middleware to protect routes using standard JWT.
func Middleware(manager *auth.Manager) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractTokenFromEcho(c)
			if tokenString == "" {
				return echoresp.Error(c, 401, "Missing or invalid Authorization header")
			}

			if manager == nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}
			claims, err := manager.ValidateTokenContext(c.Request().Context(), tokenString)
			if err != nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			// Store UserInfo in Echo's context
			userInfo := &claims.UserInfo
			c.Set(string(ctxkey.UserInfo), userInfo)
			c.Set(string(ctxkey.AuthClaims), claims.RegisteredClaims)
			ctx := ctxkey.SetAuthClaims(c.Request().Context(), claims.RegisteredClaims)
			c.SetRequest(c.Request().WithContext(ctxkey.SetUserInfo(ctx, userInfo)))
			return next(c)
		}
	}
}

// GenericMiddleware protects Echo routes with a generic token validator. The
// validated value is available from both Echo and request contexts.
func GenericMiddleware[T any](validator auth.ContextTokenValidator[T]) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractTokenFromEcho(c)
			if tokenString == "" {
				return echoresp.Error(c, 401, "Missing or invalid Authorization header")
			}
			if validator == nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			claims, err := validator.ValidateRequestToken(c.Request().Context(), tokenString)
			if err != nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			c.Set(string(ctxkey.UserInfo), claims)
			c.Set(string(ctxkey.AuthClaims), claims)
			ctx := ctxkey.SetAuthClaims(c.Request().Context(), claims)
			c.SetRequest(c.Request().WithContext(ctxkey.SetUserInfo(ctx, claims)))
			return next(c)
		}
	}
}

// EncryptedMiddleware creates an Echo middleware to protect routes using Encrypted JWT.
// aadExtractor is an optional function to extract Dynamic AAD (e.g., from a Session ID cookie).
func EncryptedMiddleware(manager *auth.EncryptedManager, aadExtractor func(c echo.Context) []byte) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractTokenFromEcho(c)
			if tokenString == "" {
				return echoresp.Error(c, 401, "Missing or invalid Authorization header")
			}

			if manager == nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}
			var aad []byte
			if aadExtractor != nil {
				aad = aadExtractor(c)
			}

			details, err := manager.ValidateTokenDetailsContext(c.Request().Context(), tokenString, aad)
			if err != nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			userInfo := &details.UserInfo
			c.Set(string(ctxkey.UserInfo), userInfo)
			c.Set(string(ctxkey.AuthClaims), details.RegisteredClaims)
			ctx := ctxkey.SetAuthClaims(c.Request().Context(), details.RegisteredClaims)
			c.SetRequest(c.Request().WithContext(ctxkey.SetUserInfo(ctx, userInfo)))
			return next(c)
		}
	}
}

func extractTokenFromEcho(c echo.Context) string {
	return authhttp.BearerToken(c.Request().Header.Get("Authorization"))
}
