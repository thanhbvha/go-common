// Package fiberlimit adapts checked rate limiting to fiber.
package fiberlimit

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	fiberresp "github.com/thanhbvha/go-common/v2/web/response/fiber"
)

type KeyGenerator func(*fiber.Ctx) string

// DefaultKeyGenerator uses the framework resolver; configure trusted proxies before serving.
func DefaultKeyGenerator(c *fiber.Ctx) string { return c.IP() }
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.MiddlewareConfig, keyGen KeyGenerator) (fiber.Handler, error) {
	if err := ratelimit.ValidateMiddleware(limiter, cfg); err != nil {
		return nil, err
	}
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}
	return func(c *fiber.Ctx) error {
		key := keyGen(c)
		var result *ratelimit.Result
		var err error
		if key == "" {
			err = errors.New("ratelimit: empty request key")
		} else {
			result, err = limiter.Allow(c.UserContext(), key, cfg.Policy)
		}
		if err != nil || result == nil {
			if err == nil {
				err = errors.New("ratelimit: backend returned no result")
			}
			if cfg.BackendFailure(c.UserContext(), err) {
				return c.Next()
			}
			return fiberresp.Error(c, http.StatusServiceUnavailable, "Rate limit service unavailable")
		}
		c.Set("X-RateLimit-Limit", strconv.Itoa(cfg.Policy.Limit))
		c.Set("X-RateLimit-Remaining", strconv.Itoa(max(0, result.Remaining)))
		c.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(result.ResetAfter).Unix(), 10))
		if !result.Allowed {
			c.Set("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(result.ResetAfter)))
			return fiberresp.Error(c, http.StatusTooManyRequests, "Too many requests")
		}
		return c.Next()
	}, nil
}
