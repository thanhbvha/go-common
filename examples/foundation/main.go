package main

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/foundation"
	"github.com/thanhbvha/go-common/health"
	"github.com/thanhbvha/go-common/health/handler/fiber"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
	// 1. Initialize Logger
	l := logger.New(logger.Options{StdOut: true})
	logger.SetDefault(l)
	defer logger.Close()

	// 2. Initialize Foundation App
	app := foundation.NewApp(foundation.Config{
		Name:            "foundation-demo",
		Version:         "v1.0.0",
		ShutdownTimeout: 10 * time.Second,
	})

	// 3. Register Health Checks
	// Let's mock a database readiness check
	app.Health().AddReadinessCheck("database", health.CustomChecker(func(ctx context.Context) error {
		// Mock logic: assuming DB is connected successfully
		return nil
	}))

	// 4. Register Application Components via OnStart
	app.OnStart(func(ctx context.Context) error {
		// Initialize your Web Server, gRPC Server, or Background Workers here
		srv := fiber.New(fiber.Config{
			DisableStartupMessage: true,
		})

		// Expose health endpoints to Kubernetes/Load Balancer
		srv.Get("/health", fiberhealth.Handler(app.Health()))
		srv.Get("/ready", fiberhealth.ReadyHandler(app.Health()))
		srv.Get("/live", fiberhealth.LiveHandler(app.Health()))

		srv.Get("/", func(c *fiber.Ctx) error {
			return c.SendString("Hello from Foundation App! (Try visiting /health)")
		})

		// Start server in a background goroutine so it doesn't block OnStart
		go func() {
			logger.Info("Starting HTTP server", "port", ":8080")
			if err := srv.Listen(":8080"); err != nil {
				logger.Error("HTTP Server stopped", "error", err)
			}
		}()

		// Important: Register how this specific component should gracefully shutdown
		app.OnShutdown(func(shutdownCtx context.Context) error {
			logger.Info("Shutting down HTTP server...")
			return srv.ShutdownWithContext(shutdownCtx)
		})

		return nil
	})

	// 5. Run the Application
	// This will execute all OnStart functions, block the main thread waiting for an OS signal,
	// and trigger all OnShutdown functions sequentially when Ctrl+C is pressed.
	if err := app.Run(); err != nil {
		logger.Error("Application failed to start", "error", err)
	}
}
