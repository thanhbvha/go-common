package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
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

// GinSuccess returns a standard success response for Gin
func GinSuccess(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:      0,
		Message:   "Success",
		Data:      data,
		RequestID: getGinRequestID(c),
	})
}

// GinError returns a standard error response for Gin
func GinError(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, Response{
		Code:      statusCode,
		Message:   message,
		RequestID: getGinRequestID(c),
	})
}

// GinValidationError returns a standard validation error response for Gin
func GinValidationError(c *gin.Context, errors interface{}) {
	c.JSON(http.StatusBadRequest, Response{
		Code:      http.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: getGinRequestID(c),
	})
}

// GinCreated returns a 201 Created response for Gin
func GinCreated(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Response{
		Code:      0,
		Message:   "Created",
		Data:      data,
		RequestID: getGinRequestID(c),
	})
}
