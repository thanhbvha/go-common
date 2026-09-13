package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/xerrors"
)

// GinErrorHandler is a custom global error handler middleware for Gin.
func GinErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			
			code := xerrors.HTTPStatusCode(err)
			stringCode := xerrors.GetCode(err)
			message := err.Error()

			if code == http.StatusInternalServerError {
				message = "An unexpected internal error occurred"
			}

			requestID := ""
			if reqID, exists := c.Get("request_id"); exists {
				if str, ok := reqID.(string); ok {
					requestID = str
				}
			}

			c.JSON(code, gin.H{
				"code":       code,
				"status":     stringCode,
				"message":    message,
				"request_id": requestID,
			})
		}
	}
}
