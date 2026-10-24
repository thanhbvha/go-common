// Package echoresp writes standardized API responses for Echo.
package echoresp

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// getEchoRequestID safely extracts the request ID from Echo's context
func getEchoRequestID(c echo.Context) string {
	if reqID := c.Get("request_id"); reqID != nil {
		if str, ok := reqID.(string); ok {
			return str
		}
	}
	if reqID, ok := ctxkey.GetRequestID(c.Request().Context()); ok {
		return reqID
	}
	return ""
}

// Success returns a standard success response for Echo
func Success(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusOK, response.NewSuccess(data, getEchoRequestID(c)))
}

// Error returns a standard error response for Echo
func Error(c echo.Context, statusCode int, message string) error {
	return c.JSON(statusCode, response.NewError(statusCode, message, getEchoRequestID(c)))
}

// ValidationError returns a standard validation error response for Echo
func ValidationError(c echo.Context, errors interface{}) error {
	return c.JSON(http.StatusBadRequest, response.NewValidationError(errors, getEchoRequestID(c)))
}

// Created returns a 201 Created response for Echo
func Created(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusCreated, response.NewCreated(data, getEchoRequestID(c)))
}

// Paginated returns a standard paginated list response for Echo.
func Paginated[T any](c echo.Context, items []T, totalRows int64, totalPages, page, size int) error {
	return c.JSON(http.StatusOK, response.NewPaginated(items, totalRows, totalPages, page, size, getEchoRequestID(c)))
}
