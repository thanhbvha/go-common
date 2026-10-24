// Package ginresp writes standardized API responses for Gin.
package ginresp

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// getGinRequestID safely extracts the request ID from Gin's context
func getGinRequestID(c *gin.Context) string {
	if reqID, exists := c.Get("request_id"); exists {
		if str, ok := reqID.(string); ok {
			return str
		}
	}
	if reqID, ok := ctxkey.GetRequestID(c.Request.Context()); ok {
		return reqID
	}
	return ""
}

// Success returns a standard success response for Gin
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, response.NewSuccess(data, getGinRequestID(c)))
}

// Error returns a standard error response for Gin
func Error(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, response.NewError(statusCode, message, getGinRequestID(c)))
}

// ValidationError returns a standard validation error response for Gin
func ValidationError(c *gin.Context, errors interface{}) {
	c.JSON(http.StatusBadRequest, response.NewValidationError(errors, getGinRequestID(c)))
}

// Created returns a 201 Created response for Gin
func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, response.NewCreated(data, getGinRequestID(c)))
}

// Paginated returns a standard paginated list response for Gin.
func Paginated[T any](c *gin.Context, items []T, totalRows int64, totalPages, page, size int) {
	c.JSON(http.StatusOK, response.NewPaginated(items, totalRows, totalPages, page, size, getGinRequestID(c)))
}
