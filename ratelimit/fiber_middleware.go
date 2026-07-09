package ratelimit

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
)

// KeyGenerator is a function that extracts a unique key (e.g., IP, User ID) from the request.
type FiberKeyGenerator func(*fiber.Ctx) string

// DefaultFiberKeyGenerator uses the Client IP as the rate limit key.
func DefaultFiberKeyGenerator(c *fiber.Ctx) string {
	return c.IP()
}

// FiberMiddleware creates a Rate Limit middleware for Fiber.
func FiberMiddleware(limiter Limiter, cfg Config, keyGen FiberKeyGenerator) fiber.Handler {
	if keyGen == nil {
		keyGen = DefaultFiberKeyGenerator
	}

	return func(c *fiber.Ctx) error {
		key := keyGen(c)
		if key == "" {
			return c.Next() // Bypass if no key could be generated
		}

		res, err := limiter.Allow(c.Context(), key, cfg)
		if err != nil {
			// If Redis is down, we usually allow the request to pass to prevent systemic failure.
			// Or we could block it depending on strictness. Here we log and bypass.
			return c.Next()
		}

		// Set standard RateLimit headers
		c.Set("X-RateLimit-Limit", strconv.Itoa(cfg.Limit))
		c.Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
		c.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(res.ResetAfter).Unix(), 10))

		if !res.Allowed {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Too many requests, please try again later.",
			})
		}

		return c.Next()
	}
}
