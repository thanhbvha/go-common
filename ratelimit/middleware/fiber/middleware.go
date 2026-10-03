package fiberlimit

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/ratelimit"
)

// KeyGenerator is a function that extracts a unique key (e.g., IP, User ID) from the request.
type KeyGenerator func(*fiber.Ctx) string

// DefaultKeyGenerator uses the Client IP as the rate limit key.
func DefaultKeyGenerator(c *fiber.Ctx) string {
	return c.IP()
}

// Middleware creates a Rate Limit middleware for Fiber.
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.Config, keyGen KeyGenerator) fiber.Handler {
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
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
