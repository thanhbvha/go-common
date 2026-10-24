// Package ginmw provides recovery, telemetry, and error middleware for Gin.
package ginmw

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/middleware/internal/recoveryhttp"
	"go.opentelemetry.io/otel/attribute"
)

// Recover prevents handler panics from crashing the process, logs a structured
// framework-independent report, and returns a safe JSON response.
func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer panicrecovery.Catch(func(report panicrecovery.Report) {
			requestID, _ := c.Get("request_id")
			requestIDString, _ := requestID.(string)
			if requestIDString == "" {
				requestIDString, _ = ctxkey.GetRequestID(c.Request.Context())
			}
			recoveryhttp.Log(c.Request.Context(), requestIDString, report)
			c.Abort()
			if !c.Writer.Written() {
				c.JSON(recoveryhttp.StatusCode, recoveryhttp.NewResponse(requestIDString))
			}
		})
		c.Next()
	}
}

// Telemetry creates an OpenTelemetry span for every incoming HTTP request in Gin.
func Telemetry(operationName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		parentCtx := telemetry.ExtractHTTPHeaders(c.Request.Context(), c.Request.Header)
		ctx, span := telemetry.StartServerSpan(parentCtx, operationName+" "+c.Request.URL.Path)
		completed := false
		defer func() {
			if !completed {
				telemetry.SetAttributes(span, attribute.Int("http.status_code", http.StatusInternalServerError))
				telemetry.RecordError(span, errors.New("panic in HTTP handler"))
			}
			span.End()
		}()

		attrs := []attribute.KeyValue{
			attribute.String("http.method", c.Request.Method),
			attribute.String("http.url", c.Request.URL.String()),
			attribute.String("http.client_ip", c.ClientIP()),
		}

		if reqID, ok := ctxkey.GetRequestID(ctx); ok {
			attrs = append(attrs, attribute.String("http.request_id", reqID))
		} else if reqID, exists := c.Get("request_id"); exists {
			if str, ok := reqID.(string); ok {
				attrs = append(attrs, attribute.String("http.request_id", str))
			}
		}

		telemetry.SetAttributes(span, attrs...)

		c.Request = c.Request.WithContext(ctx)

		c.Next()
		completed = true

		statusCode := c.Writer.Status()
		telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))

		if len(c.Errors) > 0 {
			telemetry.RecordError(span, c.Errors.Last())
		} else if statusCode >= 500 {
			telemetry.RecordError(span, errors.New(http.StatusText(statusCode)))
		}
	}
}
