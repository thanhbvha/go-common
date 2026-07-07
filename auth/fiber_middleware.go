package auth

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// FiberMiddleware creates a Fiber middleware to protect routes using standard JWT.
func FiberMiddleware(manager *Manager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := extractTokenFromFiber(c)
		if tokenString == "" {
			return response.Error(c, fiber.StatusUnauthorized, "Missing or invalid Authorization header")
		}

		userInfo, err := manager.ExtractUserInfo(tokenString)
		if err != nil {
			return response.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		// Store UserInfo in Fiber's context for downstream handlers
		// Note: Fiber's Locals expects string keys, but we can use our ctxkey constants as strings
		c.Locals(string(ctxkey.UserInfo), userInfo)

		return c.Next()
	}
}

// FiberEncryptedMiddleware creates a Fiber middleware to protect routes using Encrypted JWT.
func FiberEncryptedMiddleware(manager *EncryptedManager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := extractTokenFromFiber(c)
		if tokenString == "" {
			return response.Error(c, fiber.StatusUnauthorized, "Missing or invalid Authorization header")
		}

		userInfo, err := manager.ValidateToken(tokenString)
		if err != nil {
			return response.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		c.Locals(string(ctxkey.UserInfo), userInfo)

		return c.Next()
	}
}

func extractTokenFromFiber(c *fiber.Ctx) string {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
		return parts[1]
	}
	return ""
}
