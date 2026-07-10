package fiber_adapter

import (
	"context"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
)

// Config contains specific configurations for the Fiber Adapter
type Config struct {
	// ContextSetup allows injecting additional data from Fiber into the Context (like Request ID, UserID, DataLoaders)
	ContextSetup func(ctx context.Context, c *fiber.Ctx) context.Context
}

// NewHandler converts a standard net/http.Handler into a Fiber Handler
func NewHandler(coreHandler http.Handler, cfg Config) fiber.Handler {
	// net/http wrapper to intercept the Request and swap the Context
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Retrieve the custom context we injected into Locals
		if customCtx, ok := r.Context().Value("fiber_custom_ctx").(context.Context); ok {
			r = r.WithContext(customCtx)
		}
		coreHandler.ServeHTTP(w, r)
	})

	httpHandler := adaptor.HTTPHandler(wrapper)

	return func(c *fiber.Ctx) error {
		if cfg.ContextSetup != nil {
			newCtx := cfg.ContextSetup(c.UserContext(), c)
			// Save newCtx to Locals so the net/http wrapper can retrieve and use it
			c.Locals("fiber_custom_ctx", newCtx)
		}

		return httpHandler(c)
	}
}

// PlaygroundHandler wraps a standard net/http.HandlerFunc into a Fiber Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) fiber.Handler {
	return adaptor.HTTPHandler(corePlayground)
}
