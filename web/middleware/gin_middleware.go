package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// GinRecover enables Gin's built-in Recovery middleware to prevent crashes on panics
func GinRecover() gin.HandlerFunc {
	return gin.Recovery()
}

// GinTelemetry creates an OpenTelemetry span for every incoming HTTP request in Gin.
func GinTelemetry(operationName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		ctx, span := telemetry.StartSpan(ctx, operationName+" "+c.Request.URL.Path)
		defer span.End()

		attrs := []attribute.KeyValue{
			attribute.String("http.method", c.Request.Method),
			attribute.String("http.url", c.Request.URL.String()),
			attribute.String("http.client_ip", c.ClientIP()),
		}

		if reqID, exists := c.Get("request_id"); exists {
			if str, ok := reqID.(string); ok {
				attrs = append(attrs, attribute.String("http.request_id", str))
			}
		}

		telemetry.SetAttributes(span, attrs...)

		c.Request = c.Request.WithContext(ctx)

		c.Next()

		statusCode := c.Writer.Status()
		telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))

		if len(c.Errors) > 0 {
			telemetry.RecordError(span, c.Errors.Last())
		} else if statusCode >= 500 {
			// Record generic error if status code >= 500 but no error attached
			telemetry.SetAttributes(span, attribute.String("error", "Internal Server Error"))
		}
	}
}
