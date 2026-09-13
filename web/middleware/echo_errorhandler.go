package middleware

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/xerrors"
)

// EchoErrorHandler is a custom global error handler for Echo
func EchoErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}

	code := http.StatusInternalServerError
	stringCode := "INTERNAL_ERROR"
	message := "An unexpected internal error occurred"

	if he, ok := err.(*echo.HTTPError); ok {
		code = he.Code
		message = he.Message.(string)
		stringCode = "ECHO_ERROR"
	} else {
		code = xerrors.HTTPStatusCode(err)
		stringCode = xerrors.GetCode(err)
		message = err.Error()

		if code == http.StatusInternalServerError {
			message = "An unexpected internal error occurred"
		}
	}

	requestID := ""
	if reqID := c.Get("request_id"); reqID != nil {
		if str, ok := reqID.(string); ok {
			requestID = str
		}
	}

	c.JSON(code, map[string]interface{}{
		"code":       code,
		"status":     stringCode,
		"message":    message,
		"request_id": requestID,
	})
}
