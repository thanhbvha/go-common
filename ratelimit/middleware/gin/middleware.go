// Package ginlimit adapts checked rate limiting to gin.
package ginlimit

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	ginresp "github.com/thanhbvha/go-common/v2/web/response/gin"
)

type KeyGenerator func(*gin.Context) string

// DefaultKeyGenerator uses the framework resolver; configure trusted proxies before serving.
func DefaultKeyGenerator(c *gin.Context) string { return c.ClientIP() }
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.MiddlewareConfig, keyGen KeyGenerator) (gin.HandlerFunc, error) {
	if err := ratelimit.ValidateMiddleware(limiter, cfg); err != nil {
		return nil, err
	}
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}
	return func(c *gin.Context) {
		key := keyGen(c)
		var result *ratelimit.Result
		var err error
		if key == "" {
			err = errors.New("ratelimit: empty request key")
		} else {
			result, err = limiter.Allow(c.Request.Context(), key, cfg.Policy)
		}
		if err != nil || result == nil {
			if err == nil {
				err = errors.New("ratelimit: backend returned no result")
			}
			if cfg.BackendFailure(c.Request.Context(), err) {
				c.Next()
				return
			}
			ginresp.Error(c, http.StatusServiceUnavailable, "Rate limit service unavailable")
			c.Abort()
			return
		}
		c.Header("X-RateLimit-Limit", strconv.Itoa(cfg.Policy.Limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(max(0, result.Remaining)))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(result.ResetAfter).Unix(), 10))
		if !result.Allowed {
			c.Header("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(result.ResetAfter)))
			ginresp.Error(c, http.StatusTooManyRequests, "Too many requests")
			c.Abort()
			return
		}
		c.Next()
	}, nil
}
