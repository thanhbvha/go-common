package echomw

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// ErrorHandler is a custom global error handler for Echo
func ErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}

	requestID := ""
	if reqID := c.Get("request_id"); reqID != nil {
		if str, ok := reqID.(string); ok {
			requestID = str
		}
	}
	if requestID == "" {
		requestID, _ = ctxkey.GetRequestID(c.Request().Context())
	}

	var payload response.ErrorResponse
	if he, ok := err.(*echo.HTTPError); ok {
		payload = response.NewHTTPError(he.Code, "ECHO_ERROR", fmt.Sprint(he.Message), requestID)
	} else {
		payload = response.FromError(err, requestID)
	}

	_ = c.JSON(payload.Code, payload)
}
