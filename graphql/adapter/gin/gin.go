package gin_adapter

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Config chứa cấu hình riêng cho Gin Adapter
type Config struct {
	// ContextSetup cho phép bơm thêm dữ liệu từ Gin vào Context
	ContextSetup func(ctx context.Context, c *gin.Context) context.Context
}

// NewHandler chuyển đổi chuẩn net/http.Handler thành Gin Handler
func NewHandler(coreHandler http.Handler, cfg Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.ContextSetup != nil {
			newCtx := cfg.ContextSetup(c.Request.Context(), c)
			c.Request = c.Request.WithContext(newCtx)
		}

		coreHandler.ServeHTTP(c.Writer, c.Request)
	}
}

// PlaygroundHandler bọc chuẩn net/http.HandlerFunc thành Gin Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		corePlayground.ServeHTTP(c.Writer, c.Request)
	}
}
