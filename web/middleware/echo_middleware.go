package middleware

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// EchoRecover enables Echo's built-in Recovery middleware to prevent crashes on panics
func EchoRecover() echo.MiddlewareFunc {
	return middleware.Recover()
}

// EchoTelemetry creates an OpenTelemetry span for every incoming HTTP request in Echo.
func EchoTelemetry(operationName string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ctx := c.Request().Context()

			ctx, span := telemetry.StartSpan(ctx, operationName+" "+c.Request().URL.Path)
			defer span.End()

			attrs := []attribute.KeyValue{
				attribute.String("http.method", c.Request().Method),
				attribute.String("http.url", c.Request().URL.String()),
				attribute.String("http.client_ip", c.RealIP()),
			}

			if reqID := c.Get("request_id"); reqID != nil {
				if str, ok := reqID.(string); ok {
					attrs = append(attrs, attribute.String("http.request_id", str))
				}
			}

			telemetry.SetAttributes(span, attrs...)

			c.SetRequest(c.Request().WithContext(ctx))

			err := next(c)

			statusCode := c.Response().Status
			telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))

			if err != nil {
				telemetry.RecordError(span, err)
			} else if statusCode >= 500 {
				telemetry.SetAttributes(span, attribute.String("error", "Internal Server Error"))
			}

			return err
		}
	}
}
