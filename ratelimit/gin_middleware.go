package ratelimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GinKeyGenerator is a function that extracts a unique key from the Gin context.
type GinKeyGenerator func(*gin.Context) string

// DefaultGinKeyGenerator uses the Client IP as the rate limit key.
func DefaultGinKeyGenerator(c *gin.Context) string {
	return c.ClientIP()
}

// GinMiddleware creates a Rate Limit middleware for Gin.
func GinMiddleware(limiter Limiter, cfg Config, keyGen GinKeyGenerator) gin.HandlerFunc {
	if keyGen == nil {
		keyGen = DefaultGinKeyGenerator
	}

	return func(c *gin.Context) {
		key := keyGen(c)
		if key == "" {
			c.Next()
			return
		}

		res, err := limiter.Allow(c.Request.Context(), key, cfg)
		if err != nil {
			// Bypass if Redis is down
			c.Next()
			return
		}

		// Set standard RateLimit headers
		c.Header("X-RateLimit-Limit", strconv.Itoa(cfg.Limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
		c.Header("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(res.ResetAfter).Unix(), 10))

		if !res.Allowed {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "Too many requests, please try again later.",
			})
			return
		}

		c.Next()
	}
}
