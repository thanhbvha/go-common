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

			userInfo, err := manager.ExtractUserInfo(tokenString)
			if err != nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			// Store UserInfo in Echo's context
			c.Set(string(ctxkey.UserInfo), userInfo)
			c.SetRequest(c.Request().WithContext(ctxkey.SetUserInfo(c.Request().Context(), userInfo)))
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

			var aad []byte
			if aadExtractor != nil {
				aad = aadExtractor(c)
			}

			userInfo, err := manager.ValidateToken(tokenString, aad)
			if err != nil {
				return echoresp.Error(c, 401, "Invalid or expired token")
			}

			c.Set(string(ctxkey.UserInfo), userInfo)
			c.SetRequest(c.Request().WithContext(ctxkey.SetUserInfo(c.Request().Context(), userInfo)))
			return next(c)
		}
	}
}

func extractTokenFromEcho(c echo.Context) string {
	return authhttp.BearerToken(c.Request().Header.Get("Authorization"))
}
