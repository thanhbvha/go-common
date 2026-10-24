package fibermw

import (
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// ErrorHandler is a custom global error handler for Fiber that automatically
// unwraps xerrors and formats them into a standardized JSON response.
//
// Usage:
//
//	app := fiber.New(fiber.Config{
//	    ErrorHandler: middleware.ErrorHandler,
//	})
func ErrorHandler(c *fiber.Ctx, err error) error {
	requestID := ""
	if reqID, ok := c.Locals("request_id").(string); ok {
		requestID = reqID
	} else if reqID, ok := ctxkey.GetRequestID(c.UserContext()); ok {
		requestID = reqID
	}

	// Check if it's a Fiber-specific error (like 404 Route Not Found, 405 Method Not Allowed)
	var payload response.ErrorResponse
	if e, ok := err.(*fiber.Error); ok {
		payload = response.NewHTTPError(e.Code, "FIBER_ERROR", e.Message, requestID)
	} else {
		payload = response.FromError(err, requestID)
	}

	// Send standard JSON response
	return c.Status(payload.Code).JSON(payload)
}
