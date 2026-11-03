// Package echolimit adapts checked rate limiting to echo.
package echolimit

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	echoresp "github.com/thanhbvha/go-common/v2/web/response/echo"
)

type KeyGenerator func(echo.Context) string

// DefaultKeyGenerator uses the framework resolver; configure trusted proxies before serving.
func DefaultKeyGenerator(c echo.Context) string { return c.RealIP() }
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.MiddlewareConfig, keyGen KeyGenerator) (echo.MiddlewareFunc, error) {
	if err := ratelimit.ValidateMiddleware(limiter, cfg); err != nil {
		return nil, err
	}
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			key := keyGen(c)
			var result *ratelimit.Result
			var err error
			if key == "" {
				err = errors.New("ratelimit: empty request key")
			} else {
				result, err = limiter.Allow(c.Request().Context(), key, cfg.Policy)
			}
			if err != nil || result == nil {
				if err == nil {
					err = errors.New("ratelimit: backend returned no result")
				}
				if cfg.BackendFailure(c.Request().Context(), err) {
					return next(c)
				}
				return echoresp.Error(c, http.StatusServiceUnavailable, "Rate limit service unavailable")
			}
			c.Response().Header().Set("X-RateLimit-Limit", strconv.Itoa(cfg.Policy.Limit))
			c.Response().Header().Set("X-RateLimit-Remaining", strconv.Itoa(max(0, result.Remaining)))
			c.Response().Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(result.ResetAfter).Unix(), 10))
			if !result.Allowed {
				c.Response().Header().Set("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(result.ResetAfter)))
				return echoresp.Error(c, http.StatusTooManyRequests, "Too many requests")
			}
			return next(c)
		}
	}, nil
}
