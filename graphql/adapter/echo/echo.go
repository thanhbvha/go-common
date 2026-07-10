package echo_adapter

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Config contains specific configurations for the Echo Adapter
type Config struct {
	// ContextSetup allows injecting additional data from Echo into the Context
	ContextSetup func(ctx context.Context, c echo.Context) context.Context
}

// NewHandler converts a standard net/http.Handler into an Echo Handler
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

// PlaygroundHandler wraps a standard net/http.HandlerFunc into an Echo Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		corePlayground.ServeHTTP(c.Response(), c.Request())
		return nil
	}
}
