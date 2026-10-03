package ginresp

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/web/response"
)

// getGinRequestID safely extracts the request ID from Gin's context
func getGinRequestID(c *gin.Context) string {
	if reqID, exists := c.Get("request_id"); exists {
		if str, ok := reqID.(string); ok {
			return str
		}
	}
	return ""
}

// Success returns a standard success response for Gin
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, response.Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getGinRequestID(c),
	})
}

// Error returns a standard error response for Gin
func Error(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, response.Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getGinRequestID(c),
	})
}

// ValidationError returns a standard validation error response for Gin
func ValidationError(c *gin.Context, errors interface{}) {
	c.JSON(http.StatusBadRequest, response.Response{
		Code:      http.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getGinRequestID(c),
	})
}

// Created returns a 201 Created response for Gin
func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, response.Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getGinRequestID(c),
	})
}

// Paginated returns a standard paginated list response for Gin.
func Paginated[T any](c *gin.Context, items []T, totalRows int64, totalPages, page, size int) {
	c.JSON(http.StatusOK, response.PaginatedResponse[T]{
		Code:       0,
		Message:    "Success",
		Data:       items,
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       page,
		Size:       size,
		RequestID:  getGinRequestID(c),
	})
}
