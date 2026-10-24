// Package echomw provides recovery, telemetry, and error middleware for Echo.
package echomw

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/utils/ctxkey"
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
				if requestID == "" {
					requestID, _ = ctxkey.GetRequestID(c.Request().Context())
				}
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
			parentCtx := telemetry.ExtractHTTPHeaders(c.Request().Context(), c.Request().Header)
			ctx, span := telemetry.StartServerSpan(parentCtx, operationName+" "+c.Request().URL.Path)
			completed := false
			defer func() {
				if !completed {
					telemetry.SetAttributes(span, attribute.Int("http.status_code", http.StatusInternalServerError))
					telemetry.RecordError(span, errors.New("panic in HTTP handler"))
				}
				span.End()
			}()

			attrs := []attribute.KeyValue{
				attribute.String("http.method", c.Request().Method),
				attribute.String("http.url", c.Request().URL.String()),
				attribute.String("http.client_ip", c.RealIP()),
			}

			if reqID, ok := ctxkey.GetRequestID(ctx); ok {
				attrs = append(attrs, attribute.String("http.request_id", reqID))
			} else if reqID := c.Get("request_id"); reqID != nil {
				if str, ok := reqID.(string); ok {
					attrs = append(attrs, attribute.String("http.request_id", str))
				}
			}

			telemetry.SetAttributes(span, attrs...)

			c.SetRequest(c.Request().WithContext(ctx))

			err := next(c)
			completed = true

			statusCode := c.Response().Status
			telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))

			if err != nil {
				telemetry.RecordError(span, err)
			} else if statusCode >= 500 {
				telemetry.RecordError(span, errors.New(http.StatusText(statusCode)))
			}

			return err
		}
	}
}
