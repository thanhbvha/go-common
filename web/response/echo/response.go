package echoresp

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/web/response"
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

// Success returns a standard success response for Echo
func Success(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusOK, response.Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getEchoRequestID(c),
	})
}

// Error returns a standard error response for Echo
func Error(c echo.Context, statusCode int, message string) error {
	return c.JSON(statusCode, response.Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getEchoRequestID(c),
	})
}

// ValidationError returns a standard validation error response for Echo
func ValidationError(c echo.Context, errors interface{}) error {
	return c.JSON(http.StatusBadRequest, response.Response{
		Code:      http.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getEchoRequestID(c),
	})
}

// Created returns a 201 Created response for Echo
func Created(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusCreated, response.Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getEchoRequestID(c),
	})
}

// Paginated returns a standard paginated list response for Echo.
func Paginated[T any](c echo.Context, items []T, totalRows int64, totalPages, page, size int) error {
	return c.JSON(http.StatusOK, response.PaginatedResponse[T]{
		Code:       0,
		Message:    "Success",
		Data:       items,
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       page,
		Size:       size,
		RequestID:  getEchoRequestID(c),
	})
}
