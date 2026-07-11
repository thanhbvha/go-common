package logger

import (
	"time"

	"github.com/labstack/echo/v4"
	"github.com/google/uuid"
)

// EchoMiddleware creates a custom request logging middleware for Echo
func EchoMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			path := c.Path()
			// Skip logging for unnecessary paths
			if path == "/favicon.ico" || path == "/healthz" {
				return next(c)
			}

			start := time.Now()

			// Execute next request
			err := next(c)

			duration := time.Since(start)

			// Get Request ID from Header or context
			requestID := c.Request().Header.Get("X-Request-ID")
			if requestID == "" {
				if id, ok := c.Get("request_id").(string); ok {
					requestID = id
				}
			}

			// Get request & response body safely (Empty by default to save memory)
			reqBody := []byte{}
			respBody := []byte{}

			// Initialize Entry
			entry := LogEntry{
				Time:      time.Now().Format(time.RFC3339),
				RequestID: requestID,
				Method:    c.Request().Method,
				Path:      path,
				Status:    c.Response().Status,
				Latency:   duration.String(),
				IP:        c.RealIP(),
				ServerIP:  localServerIP,
				UserAgent: c.Request().UserAgent(),
				Request:   safeStringBytes(reqBody, 1024),
				Response:  safeStringBytes(respBody, 2048),
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
}

// EchoRequestIDMiddleware adds a unique X-Request-ID to each request in Echo
func EchoRequestIDMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			requestID := c.Request().Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = uuid.New().String()
			}

			c.Set("request_id", requestID)
			c.Response().Header().Set("X-Request-ID", requestID)

			return next(c)
		}
	}
}
