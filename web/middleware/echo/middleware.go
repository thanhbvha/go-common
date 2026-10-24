package echomw

import (
	"github.com/labstack/echo/v4"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/web/middleware/internal/recoveryhttp"
	"go.opentelemetry.io/otel/attribute"
)

// Recover prevents handler panics from crashing the process, logs a structured
// framework-independent report, and returns a safe JSON response.
func Recover() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			defer panicrecovery.Catch(func(report panicrecovery.Report) {
				requestID, _ := c.Get("request_id").(string)
				recoveryhttp.Log(c.Request().Context(), requestID, report)
				if !c.Response().Committed {
					err = c.JSON(recoveryhttp.StatusCode, recoveryhttp.NewResponse(requestID))
				}
			})
			return next(c)
		}
	}
}

// Telemetry creates an OpenTelemetry span for every incoming HTTP request in Echo.
func Telemetry(operationName string) echo.MiddlewareFunc {
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
