package response

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// getEchoRequestID safely extracts the request ID from Echo's context
func getEchoRequestID(c echo.Context) string {
	if reqID := c.Get("request_id"); reqID != nil {
		if str, ok := reqID.(string); ok {
			return str
		}
	}
	return ""
}

// EchoSuccess returns a standard success response for Echo
func EchoSuccess(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusOK, Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getEchoRequestID(c),
	})
}

// EchoError returns a standard error response for Echo
func EchoError(c echo.Context, statusCode int, message string) error {
	return c.JSON(statusCode, Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getEchoRequestID(c),
	})
}

// EchoValidationError returns a standard validation error response for Echo
func EchoValidationError(c echo.Context, errors interface{}) error {
	return c.JSON(http.StatusBadRequest, Response{
		Code:      http.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getEchoRequestID(c),
	})
}

// EchoCreated returns a 201 Created response for Echo
func EchoCreated(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusCreated, Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getEchoRequestID(c),
	})
}
