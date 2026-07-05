package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/xerrors"
	"go.opentelemetry.io/otel/attribute"
)

// RequestID enables Fiber's built-in RequestID middleware
func RequestID() fiber.Handler {
	return requestid.New()
}

// Recover enables Fiber's built-in Recover middleware to prevent crashes on panics
func Recover() fiber.Handler {
	return recover.New()
}

// Telemetry creates an OpenTelemetry span for every incoming HTTP request.
// It automatically extracts standard HTTP attributes and attaches them to the span.
func Telemetry(operationName string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Use the context from the request
		ctx := c.UserContext()

		// Start a new span
		ctx, span := telemetry.StartSpan(ctx, operationName+" "+c.Path())
		defer span.End()

		// Add basic HTTP attributes
		telemetry.SetAttributes(span,
			attribute.String("http.method", c.Method()),
			attribute.String("http.url", c.OriginalURL()),
			attribute.String("http.client_ip", c.IP()),
		)

		// Inject the new trace context back into the fiber request
		// so that downstream handlers and DB calls can use it.
		// Note: Fiber ctx is not context.Context, it holds it in UserContext()
		c.SetUserContext(ctx)

		// Continue processing the request
		err := c.Next()

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
