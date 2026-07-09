package echo_adapter

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Config chứa cấu hình riêng cho Echo Adapter
type Config struct {
	// ContextSetup cho phép bơm thêm dữ liệu từ Echo vào Context
	ContextSetup func(ctx context.Context, c echo.Context) context.Context
}

// NewHandler chuyển đổi chuẩn net/http.Handler thành Echo Handler
func NewHandler(coreHandler http.Handler, cfg Config) echo.HandlerFunc {
	return func(c echo.Context) error {
		req := c.Request()
		if cfg.ContextSetup != nil {
			newCtx := cfg.ContextSetup(req.Context(), c)
			req = req.WithContext(newCtx)
			c.SetRequest(req)
		}

		coreHandler.ServeHTTP(c.Response(), req)
		return nil
	}
}

// PlaygroundHandler bọc chuẩn net/http.HandlerFunc thành Echo Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		corePlayground.ServeHTTP(c.Response(), c.Request())
		return nil
	}
}
