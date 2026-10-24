// package fibermw provides standard HTTP middlewares for the Fiber web framework.
//
// It includes essential middlewares for production applications such as RequestID,
// Panic Recovery, and OpenTelemetry instrumentation for incoming HTTP requests.
//
// Basic usage:
//
//	app := fiber.New()
//	app.Use(middleware.RequestID())
//	app.Use(middleware.Recover())
//	app.Use(middleware.Telemetry("my-service"))
package fibermw

import (
	"errors"
	"net/http"

	"github.com/gofiber/fiber/v2"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/middleware/internal/recoveryhttp"
	"github.com/thanhbvha/go-common/xerrors"
	"go.opentelemetry.io/otel/attribute"
)

// Recover prevents handler panics from crashing the process, logs a structured
// framework-independent report, and returns a safe JSON response.
func Recover() fiber.Handler {
	return func(c *fiber.Ctx) (err error) {
		defer panicrecovery.Catch(func(report panicrecovery.Report) {
			requestID, _ := c.Locals("request_id").(string)
			if requestID == "" {
				requestID, _ = ctxkey.GetRequestID(c.UserContext())
			}
			recoveryhttp.Log(c.UserContext(), requestID, report)
			err = c.Status(recoveryhttp.StatusCode).JSON(recoveryhttp.NewResponse(requestID))
		})
		return c.Next()
	}
}

// Telemetry creates an OpenTelemetry span for every incoming HTTP request.
// It automatically extracts standard HTTP attributes and attaches them to the span.
func Telemetry(operationName string) fiber.Handler {
	return func(c *fiber.Ctx) (err error) {
		headers := make(http.Header)
		c.Request().Header.VisitAll(func(key, value []byte) {
			headers.Add(string(key), string(value))
		})
		parentCtx := telemetry.ExtractHTTPHeaders(c.UserContext(), headers)
		ctx, span := telemetry.StartServerSpan(parentCtx, operationName+" "+c.Path())
		completed := false
		defer func() {
			if !completed {
				telemetry.SetAttributes(span, attribute.Int("http.status_code", http.StatusInternalServerError))
				telemetry.RecordError(span, errors.New("panic in HTTP handler"))
			}
			span.End()
		}()

		// Add basic HTTP attributes
		attrs := []attribute.KeyValue{
			attribute.String("http.method", c.Method()),
			attribute.String("http.url", c.OriginalURL()),
			attribute.String("http.client_ip", c.IP()),
		}

		if reqID, ok := ctxkey.GetRequestID(ctx); ok {
			attrs = append(attrs, attribute.String("http.request_id", reqID))
		} else if reqID, ok := c.Locals("request_id").(string); ok {
			attrs = append(attrs, attribute.String("http.request_id", reqID))
		}

		telemetry.SetAttributes(span, attrs...)

		// Inject the new trace context back into the fiber request
		// so that downstream handlers and DB calls can use it.
		// Note: Fiber ctx is not context.Context, it holds it in UserContext()
		c.SetUserContext(ctx)

		// Continue processing the request
		err = c.Next()
		completed = true

		// Record correct status code (handle Fiber errors that haven't been processed by the global error handler yet)
		statusCode := c.Response().StatusCode()
		if err != nil {
			if e, ok := err.(*fiber.Error); ok {
				statusCode = e.Code
			} else {
				// Safely extracts HTTP status from xerrors, defaults to 500
				statusCode = xerrors.HTTPStatusCode(err)
			}
		}

		telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))

		if err != nil {
			telemetry.RecordError(span, err)
			return err
		}

		if statusCode >= 500 {
			telemetry.RecordError(span, fiber.NewError(statusCode, "Internal Server Error"))
		}

		return nil
	}
}
