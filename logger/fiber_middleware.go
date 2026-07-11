package logger

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// FiberMiddleware creates a custom request logging middleware for Fiber
func FiberMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		path := c.Path()
		// Skip logging for unnecessary paths
		if path == "/favicon.ico" || path == "/healthz" {
			return c.Next()
		}

		start := time.Now()

		// Execute next request
		err := c.Next()

		duration := time.Since(start)

		// Get Request ID from Header or Locals
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			if id, ok := c.Locals("request_id").(string); ok {
				requestID = id
			}
		}

		// Get request & response body safely
		// reqBody := c.Body()
		// respBody := c.Response().Body()
		reqBody := []byte{}
		respBody := []byte{}

		// Initialize Entry
		entry := LogEntry{
			Time:      time.Now().Format(time.RFC3339),
			RequestID: requestID,
			Method:    c.Method(),
			Path:      path,
			Status:    c.Response().StatusCode(),
			Latency:   duration.String(),
			IP:        c.IP(),
			ServerIP:  localServerIP,
			UserAgent: c.Get(fiber.HeaderUserAgent),
			Request:   safeStringBytes(reqBody, 1024),  // Take only the first 1KB
			Response:  safeStringBytes(respBody, 2048), // Take only the first 2KB
		}

		// Log via the core logger library
		if err != nil {
			entry.Error = err.Error()
			ErrorAsync("HTTP Request Failed", "entry", entry)
		} else {
			InfoAsync("HTTP Request OK", "entry", entry)
		}

		return err
	}
}

// FiberRequestIDMiddleware adds a unique X-Request-ID to each request in Fiber
func FiberRequestIDMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get request id from header if exists
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Attach to context for use throughout the request lifecycle
		c.Locals("request_id", requestID)

		// Set header in response for client
		c.Set("X-Request-ID", requestID)

		// Continue
		return c.Next()
	}
}
