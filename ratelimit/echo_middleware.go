package ratelimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
)

// EchoKeyGenerator is a function that extracts a unique key from the Echo context.
type EchoKeyGenerator func(echo.Context) string

// DefaultEchoKeyGenerator uses the Client IP as the rate limit key.
func DefaultEchoKeyGenerator(c echo.Context) string {
	return c.RealIP()
}

// EchoMiddleware creates a Rate Limit middleware for Echo.
func EchoMiddleware(limiter Limiter, cfg Config, keyGen EchoKeyGenerator) echo.MiddlewareFunc {
	if keyGen == nil {
		keyGen = DefaultEchoKeyGenerator
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
