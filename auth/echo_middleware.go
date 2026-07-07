package auth

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

// EchoMiddleware creates an Echo middleware to protect routes using standard JWT.
func EchoMiddleware(manager *Manager) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractTokenFromEcho(c)
			if tokenString == "" {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "Missing or invalid Authorization header",
				})
			}

			userInfo, err := manager.ExtractUserInfo(tokenString)
			if err != nil {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "Invalid or expired token",
				})
			}

			// Store UserInfo in Echo's context
			c.Set(string(ctxkey.UserInfo), userInfo)
			return next(c)
		}
	}
}

// EchoEncryptedMiddleware creates an Echo middleware to protect routes using Encrypted JWT.
func EchoEncryptedMiddleware(manager *EncryptedManager) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractTokenFromEcho(c)
			if tokenString == "" {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "Missing or invalid Authorization header",
				})
			}

			userInfo, err := manager.ValidateToken(tokenString)
			if err != nil {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "Invalid or expired token",
				})
			}

			c.Set(string(ctxkey.UserInfo), userInfo)
			return next(c)
		}
	}
}

func extractTokenFromEcho(c echo.Context) string {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
		return parts[1]
	}
	return ""
}
