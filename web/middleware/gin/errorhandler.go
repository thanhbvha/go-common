package ginmw

import (
	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// ErrorHandler is a custom global error handler middleware for Gin.
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err

			requestID := ""
			if reqID, exists := c.Get("request_id"); exists {
				if str, ok := reqID.(string); ok {
					requestID = str
				}
			}
			if requestID == "" {
				requestID, _ = ctxkey.GetRequestID(c.Request.Context())
			}

			payload := response.FromError(err, requestID)
			c.JSON(payload.Code, payload)
		}
	}
}
