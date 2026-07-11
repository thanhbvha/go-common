package logger

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// GinMiddleware creates a custom request logging middleware for Gin
func GinMiddleware() gin.HandlerFunc {
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
		entry := LogEntry{
			Time:      time.Now().Format(time.RFC3339),
			RequestID: requestID,
			Method:    c.Request.Method,
			Path:      path,
			Status:    c.Writer.Status(),
			Latency:   duration.String(),
			IP:        c.ClientIP(),
			ServerIP:  localServerIP,
			UserAgent: c.Request.UserAgent(),
			Request:   safeStringBytes(reqBody, 1024),
			Response:  safeStringBytes(respBody, 2048),
		}

		// Log via the core logger library
		if len(c.Errors) > 0 {
			entry.Error = c.Errors.String()
			ErrorAsync("HTTP Request Failed", "entry", entry)
		} else {
			InfoAsync("HTTP Request OK", "entry", entry)
		}
	}
}

// GinRequestIDMiddleware adds a unique X-Request-ID to each request in Gin
func GinRequestIDMiddleware() gin.HandlerFunc {
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
