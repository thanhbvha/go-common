package echolimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/ratelimit"
)

// KeyGenerator is a function that extracts a unique key from the Echo context.
type KeyGenerator func(echo.Context) string

// DefaultKeyGenerator uses the Client IP as the rate limit key.
func DefaultKeyGenerator(c echo.Context) string {
	return c.RealIP()
}

// Middleware creates a Rate Limit middleware for Echo.
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.Config, keyGen KeyGenerator) echo.MiddlewareFunc {
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			key := keyGen(c)
			if key == "" {
				return next(c)
			}

			res, err := limiter.Allow(c.Request().Context(), key, cfg)
			if err != nil {
				// Bypass if Redis is down
				return next(c)
			}

			// Set standard RateLimit headers
			c.Response().Header().Set("X-RateLimit-Limit", strconv.Itoa(cfg.Limit))
			c.Response().Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			c.Response().Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(res.ResetAfter).Unix(), 10))

			if !res.Allowed {
				return c.JSON(http.StatusTooManyRequests, map[string]string{
					"error": "Too many requests, please try again later.",
				})
			}

			return next(c)
		}
	}
}
