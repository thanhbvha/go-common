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

		if manager == nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}
		claims, err := manager.ValidateTokenContext(c.UserContext(), tokenString)
		if err != nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		// Store UserInfo in Fiber's context for downstream handlers
		// Note: Fiber's Locals expects string keys, but we can use our ctxkey constants as strings
		userInfo := &claims.UserInfo
		c.Locals(string(ctxkey.UserInfo), userInfo)
		c.Locals(string(ctxkey.AuthClaims), claims.RegisteredClaims)
		ctx := ctxkey.SetAuthClaims(c.UserContext(), claims.RegisteredClaims)
		c.SetUserContext(ctxkey.SetUserInfo(ctx, userInfo))

		return c.Next()
	}
}

// GenericMiddleware protects Fiber routes with a generic token validator. The
// validated value is available from both Locals and UserContext.
func GenericMiddleware[T any](validator auth.ContextTokenValidator[T]) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenString := extractTokenFromFiber(c)
		if tokenString == "" {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Missing or invalid Authorization header")
		}
		if validator == nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		claims, err := validator.ValidateRequestToken(c.UserContext(), tokenString)
		if err != nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		c.Locals(string(ctxkey.UserInfo), claims)
		c.Locals(string(ctxkey.AuthClaims), claims)
		ctx := ctxkey.SetAuthClaims(c.UserContext(), claims)
		c.SetUserContext(ctxkey.SetUserInfo(ctx, claims))
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

		if manager == nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}
		var aad []byte
		if aadExtractor != nil {
			aad = aadExtractor(c)
		}

		details, err := manager.ValidateTokenDetailsContext(c.UserContext(), tokenString, aad)
		if err != nil {
			return fiberresp.Error(c, fiber.StatusUnauthorized, "Invalid or expired token")
		}

		userInfo := &details.UserInfo
		c.Locals(string(ctxkey.UserInfo), userInfo)
		c.Locals(string(ctxkey.AuthClaims), details.RegisteredClaims)
		ctx := ctxkey.SetAuthClaims(c.UserContext(), details.RegisteredClaims)
		c.SetUserContext(ctxkey.SetUserInfo(ctx, userInfo))

		return c.Next()
	}
}

func extractTokenFromFiber(c *fiber.Ctx) string {
	return authhttp.BearerToken(c.Get("Authorization"))
}
