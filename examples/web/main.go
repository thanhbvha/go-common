package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
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
	// 1. Initialize Telemetry
	tel, _ := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:   "demo-web-api",
		EnableTracing: true,
		Endpoint:      "localhost:4317",
	})
	graceful.Register(func(ctx context.Context) error {
		log.Println("Shutting down telemetry...")
		return tel.Shutdown(ctx)
	})

	// 2. Initialize Fiber App
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// 3. Setup Middlewares
	app.Use(middleware.Recover())
	app.Use(middleware.RequestID())
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
		fmt.Printf("Server is starting on http://localhost%s\n", port)
		if err := app.Listen(port); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Register Fiber shutdown
	graceful.Register(func(ctx context.Context) error {
		log.Println("Shutting down Fiber server...")
		return app.ShutdownWithContext(ctx)
	})

	// 6. Block and wait for OS signals (CTRL+C)
	fmt.Println("Press CTRL+C to stop the server...")
	graceful.Wait(10 * time.Second)
}
