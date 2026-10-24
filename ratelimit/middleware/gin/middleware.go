// Package ginlimit provides rate limiting middleware for Gin.
package ginlimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/ratelimit"
	ginresp "github.com/thanhbvha/go-common/web/response/gin"
)

// KeyGenerator is a function that extracts a unique key from the Gin context.
type KeyGenerator func(*gin.Context) string

// DefaultKeyGenerator uses the Client IP as the rate limit key.
func DefaultKeyGenerator(c *gin.Context) string {
	return c.ClientIP()
}

// Middleware creates a Rate Limit middleware for Gin.
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.Config, keyGen KeyGenerator) gin.HandlerFunc {
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}

	return func(c *gin.Context) {
		key := keyGen(c)
		if key == "" {
			c.Next()
			return
		}

		res, err := limiter.Allow(c.Request.Context(), key, cfg)
		if err != nil || res == nil {
			// Bypass if Redis is down
			c.Next()
			return
		}

		// Set standard RateLimit headers
		c.Header("X-RateLimit-Limit", strconv.Itoa(cfg.Limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(res.ResetAfter).Unix(), 10))

		if !res.Allowed {
			ginresp.Error(c, http.StatusTooManyRequests, "Too many requests, please try again later.")
			c.Abort()
			return
		}

		c.Next()
	}
}
