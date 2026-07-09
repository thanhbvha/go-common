package fiber_adapter

import (
	"context"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
)

// Config chứa cấu hình riêng cho Fiber Adapter
type Config struct {
	// ContextSetup cho phép bơm thêm dữ liệu từ Fiber vào Context (như Request ID, UserID, DataLoaders)
	ContextSetup func(ctx context.Context, c *fiber.Ctx) context.Context
}

// NewHandler chuyển đổi chuẩn net/http.Handler thành Fiber Handler
func NewHandler(coreHandler http.Handler, cfg Config) fiber.Handler {
	httpHandler := adaptor.HTTPHandler(coreHandler)

	return func(c *fiber.Ctx) error {
		if cfg.ContextSetup != nil {
			newCtx := cfg.ContextSetup(c.UserContext(), c)
			c.SetUserContext(newCtx)
		}

		return httpHandler(c)
	}
}

// PlaygroundHandler bọc chuẩn net/http.HandlerFunc thành Fiber Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) fiber.Handler {
	return adaptor.HTTPHandler(corePlayground)
}
