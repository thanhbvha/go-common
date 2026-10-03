package ginlog

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/logger"
)

// Middleware creates a custom request logging middleware for Gin
func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		// Skip logging for unnecessary paths
		if path == "/favicon.ico" || path == "/healthz" {
			c.Next()
			return
		}

		start := time.Now()

		// Execute next request
		c.Next()

		duration := time.Since(start)

		// Get Request ID from Header or context keys
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			if id, exists := c.Get("request_id"); exists {
				if idStr, ok := id.(string); ok {
					requestID = idStr
				}
			}
		}

		// Get request & response body safely (Empty by default to save memory)
		reqBody := []byte{}
		respBody := []byte{}

		// Initialize Entry
		entry := logger.LogEntry{
			Time:      time.Now().Format(time.RFC3339),
			RequestID: requestID,
			Method:    c.Request.Method,
			Path:      path,
			Status:    c.Writer.Status(),
			Latency:   duration.String(),
			IP:        c.ClientIP(),
			ServerIP:  logger.LocalServerIP,
			UserAgent: c.Request.UserAgent(),
			Request:   logger.SafeStringBytes(reqBody, 1024),
			Response:  logger.SafeStringBytes(respBody, 2048),
		}

		// Log via the core logger library
		if len(c.Errors) > 0 {
			entry.Error = c.Errors.String()
			logger.ErrorAsync("HTTP Request Failed", "entry", entry)
		} else {
			logger.InfoAsync("HTTP Request OK", "entry", entry)
		}
	}
}

// RequestIDMiddleware adds a unique X-Request-ID to each request in Gin
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		c.Set("request_id", requestID)
		c.Writer.Header().Set("X-Request-ID", requestID)

		c.Next()
	}
}
