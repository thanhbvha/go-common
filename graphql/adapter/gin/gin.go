package gin_adapter

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Config contains specific configurations for the Gin Adapter
type Config struct {
	// ContextSetup allows injecting additional data from Gin into the Context
	ContextSetup func(ctx context.Context, c *gin.Context) context.Context
}

// NewHandler converts a standard net/http.Handler into a Gin Handler
func NewHandler(coreHandler http.Handler, cfg Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.ContextSetup != nil {
			newCtx := cfg.ContextSetup(c.Request.Context(), c)
			c.Request = c.Request.WithContext(newCtx)
		}

		coreHandler.ServeHTTP(c.Writer, c.Request)
	}
}

// PlaygroundHandler wraps a standard net/http.HandlerFunc into a Gin Handler
func PlaygroundHandler(corePlayground http.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		corePlayground.ServeHTTP(c.Writer, c.Request)
	}
}
