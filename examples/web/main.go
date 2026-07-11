package main

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/utils/graceful"
	"github.com/thanhbvha/go-common/utils/str"
	"github.com/thanhbvha/go-common/web/middleware"
	"github.com/thanhbvha/go-common/web/response"
	"github.com/thanhbvha/go-common/web/validator"
)

type CreateUserRequest struct {
	Name  string `json:"name" validate:"required,min=3"`
	Email string `json:"email" validate:"required,email"`
}

func main() {
	// Initialize standard logger from go-common
	l := logger.New(logger.Options{
		StdOut: true,
	})
	logger.SetDefault(l)
	defer logger.Close()

	// 1. Initialize Telemetry
	tel, _ := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:   "demo-web-api",
		EnableTracing: false,
		Endpoint:      "localhost:4317",
	})
	graceful.Register(func(ctx context.Context) error {
		log.Println("Shutting down telemetry...")
		return tel.Shutdown(ctx)
	})

	// 2. Initialize Fiber App
	app := fiber.New(fiber.Config{
		// DisableStartupMessage: true,
		ErrorHandler: middleware.ErrorHandler,
	})

	// 3. Setup Middlewares
	app.Use(middleware.Recover())
	app.Use(logger.FiberRequestIDMiddleware())
	app.Use(logger.FiberMiddleware())
	app.Use(middleware.Telemetry("HTTP Request"))

	// 4. Define Routes
	app.Post("/users", func(c *fiber.Ctx) error {
		var req CreateUserRequest

		// Parse Body
		if err := c.BodyParser(&req); err != nil {
			return response.Error(c, fiber.StatusBadRequest, "Invalid input data")
		}

		// Validate Body
		if errs := validator.Struct(&req); errs != nil {
			return response.ValidationError(c, errs)
		}

		// Use String Util to generate a random password and slugify name
		slugName := str.Slugify(req.Name)
		tempPassword := str.Random(8)

		// Return standardized success response
		return response.Created(c, fiber.Map{
			"slug":     slugName,
			"password": tempPassword,
			"email":    req.Email,
		})
	})

	// 5. Start Server in a goroutine
	go func() {
		port := ":3000"
		logger.Info("Server is starting", "port", port)
		if err := app.Listen(port); err != nil {
			logger.Error("Server error", "err", err)
		}
	}()

	// Register Fiber shutdown
	graceful.Register(func(ctx context.Context) error {
		logger.Info("Shutting down Fiber server...")
		return app.ShutdownWithContext(ctx)
	})

	// 6. Block and wait for OS signals (CTRL+C)
	graceful.Wait(10 * time.Second)
}
