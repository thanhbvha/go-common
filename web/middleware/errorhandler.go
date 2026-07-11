package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/xerrors"
)

// ErrorHandler is a custom global error handler for Fiber that automatically
// unwraps xerrors and formats them into a standardized JSON response.
//
// Usage:
// app := fiber.New(fiber.Config{
//     ErrorHandler: middleware.ErrorHandler,
// })
func ErrorHandler(c *fiber.Ctx, err error) error {
	// Default to 500
	code := fiber.StatusInternalServerError
	stringCode := "INTERNAL_ERROR"
	message := "An unexpected internal error occurred"

	// Check if it's a Fiber-specific error (like 404 Route Not Found, 405 Method Not Allowed)
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
		message = e.Message
		stringCode = "FIBER_ERROR"
	} else {
		// Check if it's our Custom xerrors
		code = xerrors.HTTPStatusCode(err)
		stringCode = xerrors.GetCode(err)
		message = err.Error()

		// Do not expose raw panics/internal errors to the client
		if code == fiber.StatusInternalServerError {
			message = "An unexpected internal error occurred"
		}
	}

	// Extract Request ID if available
	requestID := ""
	if reqID, ok := c.Locals("request_id").(string); ok {
		requestID = reqID
	}

	// Send standard JSON response
	return c.Status(code).JSON(fiber.Map{
		"code":       code,       // Numeric status code
		"status":     stringCode, // String status code from xerrors
		"message":    message,
		"request_id": requestID,
	})
}
