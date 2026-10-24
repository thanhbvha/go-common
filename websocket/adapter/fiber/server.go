// Package fiber provides a WebSocket server adapter for the Fiber web framework.
//
// It wraps the generic WebSocket core manager into a fully-functional, high-performance
// HTTP server using github.com/gofiber/fiber/v2, enabling easy integration of real-time
// capabilities into existing Fiber applications.
package fiber

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/logger"
)

const (
	defaultPort    = 8000
	readTimeout    = 10 * time.Second
	writeTimeout   = 10 * time.Second
	idleTimeout    = 120 * time.Second
	maxRequestSize = 1 << 20 // 1MB
)

// Server wraps the Fiber framework app to bootstrap the high-performance WebSocket server.
type Server struct {
	app     *fiber.App
	port    int
	handler *Handler
}

// NewServer builds a configured Fiber server instance integrated with our WebSocket Handler.
// It accepts an optional variadic custom fiber.Config. If provided, the custom configurations
// are merged with the sensible performance defaults.
func NewServer(port int, handler *Handler, customConfig ...fiber.Config) *Server {
	if port <= 0 {
		port = defaultPort
	}

	var config fiber.Config
	if len(customConfig) > 0 {
		config = customConfig[0]
	}

	// Apply sensible performance defaults if fields are not specified by user
	if config.ReadTimeout == 0 {
		config.ReadTimeout = readTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = writeTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = idleTimeout
	}
	if config.BodyLimit <= 0 {
		config.BodyLimit = maxRequestSize
	}
	if config.ServerHeader == "" {
		config.ServerHeader = "WebSocket-Server"
	}
	if config.AppName == "" {
		config.AppName = "WebSocket Service"
	}
	if config.ErrorHandler == nil {
		config.ErrorHandler = func(ctx *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			logger.ErrorAsync("Fiber HTTP error occurred", "error", err.Error(), "status", code, "path", ctx.Path())
			message := err.Error()
			if code >= fiber.StatusInternalServerError {
				message = http.StatusText(http.StatusInternalServerError)
			}
			return ctx.Status(code).JSON(fiber.Map{"error": message})
		}
	}

	app := fiber.New(config)

	return &Server{
		app:     app,
		port:    port,
		handler: handler,
	}
}

// SetupRoutes binds only the public upgrade and health routes. Operational
// routes are intentionally left to the application so it can enforce its own
// administrator authentication policy.
func (s *Server) SetupRoutes() {
	// WebSocket gateway upgrade endpoint
	s.app.Get("/ws", s.handler.HandleUpgrade)

	s.app.Get("/health", s.handler.HandleHealthCheck)
}

// Start launches the Fiber listener loop on a separate goroutine and reports startup health.
func (s *Server) Start() error {
	logger.InfoAsync("Starting Fiber WebSocket server...", "port", s.port)

	errChan := make(chan error, 1)
	go func() {
		if err := s.app.Listen(":" + strconv.Itoa(s.port)); err != nil {
			errChan <- fmt.Errorf("fiber app failed to start: %w", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(500 * time.Millisecond):
		logger.InfoAsync("WebSocket Fiber server started successfully", "port", s.port)
		return nil
	}
}

// Shutdown gracefully stops the underlying Fiber server instance.
func (s *Server) Shutdown() error {
	logger.InfoAsync("Gracefully shutting down WebSocket server...")
	return s.app.Shutdown()
}

// App returns the raw, underlying Fiber application instance.
func (s *Server) App() *fiber.App {
	return s.app
}
