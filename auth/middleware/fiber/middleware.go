// Package fiberauth provides JWT authentication middleware for Fiber.
package fiberauth

import (
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/auth"
	"github.com/thanhbvha/go-common/auth/middleware/internal/authhttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	fiberresp "github.com/thanhbvha/go-common/web/response/fiber"
)

// Middleware creates a Fiber middleware to protect routes using standard JWT.
func Middleware(manager *auth.Manager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := extractTokenFromFiber(c)
		if tokenString == "" {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Missing or invalid Authorization header")
		}

		userInfo, err := manager.ExtractUserInfo(tokenString)
		if err != nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		// Store UserInfo in Fiber's context for downstream handlers
		// Note: Fiber's Locals expects string keys, but we can use our ctxkey constants as strings
		c.Locals(string(ctxkey.UserInfo), userInfo)
		c.SetUserContext(ctxkey.SetUserInfo(c.UserContext(), userInfo))

		return c.Next()
	}
}

// EncryptedMiddleware creates a Fiber middleware to protect routes using Encrypted JWT.
// aadExtractor is an optional function to extract Dynamic AAD (e.g., from a Session ID cookie).
func EncryptedMiddleware(manager *auth.EncryptedManager, aadExtractor func(c *fiber.Ctx) []byte) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := extractTokenFromFiber(c)
		if tokenString == "" {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Missing or invalid Authorization header")
		}

		var aad []byte
		if aadExtractor != nil {
			aad = aadExtractor(c)
		}

		userInfo, err := manager.ValidateToken(tokenString, aad)
		if err != nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		c.Locals(string(ctxkey.UserInfo), userInfo)
		c.SetUserContext(ctxkey.SetUserInfo(c.UserContext(), userInfo))

		return c.Next()
	}
}

func extractTokenFromFiber(c *fiber.Ctx) string {
	return authhttp.BearerToken(c.Get("Authorization"))
}
