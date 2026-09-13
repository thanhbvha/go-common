package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	
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
	fmt.Println("=== Web API Module Examples ===")

	// Initialize standard logger from go-common
	l := logger.New(logger.Options{StdOut: true})
	logger.SetDefault(l)
	defer logger.Close()

	// Initialize Telemetry
	tel, _ := telemetry.Init(context.Background(), telemetry.Config{
		ServiceName:   "demo-web-api",
		EnableTracing: false,
		Endpoint:      "localhost:4317",
	})
	graceful.Register(func(ctx context.Context) error {
		log.Println("Shutting down telemetry...")
		return tel.Shutdown(ctx)
	})

	// Uncomment the example you want to run:
	RunFiberAPIExample()
	// RunGinAPIExample()
	// RunEchoAPIExample()

	// Block and wait for OS signals (CTRL+C)
	// CRITICAL: Always use graceful.Wait() in web APIs. It blocks the main thread
	// until a SIGINT/SIGTERM is received, then gives active requests time to finish.
	graceful.Wait(10 * time.Second)
}

// =====================================================================
// 1. Fiber REST API Example
// =====================================================================
func RunFiberAPIExample() {
	fmt.Println("\n--- 1. Fiber REST API ---")
	app := fiber.New(fiber.Config{
		ErrorHandler: middleware.ErrorHandler,
	})

	// Setup Middlewares
	app.Use(middleware.Recover())
	app.Use(logger.FiberRequestIDMiddleware())
	app.Use(logger.FiberMiddleware())
	app.Use(middleware.Telemetry("HTTP Request"))

	// Define Routes
	app.Post("/users", func(c *fiber.Ctx) error {
		var req CreateUserRequest
		if err := c.BodyParser(&req); err != nil {
			return response.Error(c, fiber.StatusBadRequest, "Invalid input data")
		}

		if errs := validator.Struct(&req); errs != nil {
			return response.ValidationError(c, errs)
		}

		slugName := str.Slugify(req.Name)
		tempPassword := str.Random(8)

		return response.Created(c, fiber.Map{
			"slug":     slugName,
			"password": tempPassword,
			"email":    req.Email,
			"framework": "Fiber",
		})
	})

	// Start Server in a goroutine
	go func() {
		port := ":3000"
		logger.Info("Fiber Server is starting", "port", port)
		if err := app.Listen(port); err != nil {
			logger.Error("Fiber Server error", "err", err)
		}
	}()

	// Register Fiber shutdown
	graceful.Register(func(ctx context.Context) error {
		logger.Info("Shutting down Fiber server...")
		return app.ShutdownWithContext(ctx)
	})
}

// =====================================================================
// 2. Gin REST API Example
// =====================================================================
func RunGinAPIExample() {
	fmt.Println("\n--- 2. Gin REST API ---")
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// Setup Middlewares
	r.Use(middleware.GinRecover())
	r.Use(middleware.GinErrorHandler())
	r.Use(middleware.GinTelemetry("HTTP Request"))

	// Define Routes
	r.POST("/users", func(c *gin.Context) {
		var req CreateUserRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			response.GinError(c, http.StatusBadRequest, "Invalid input data")
			return
		}

		if errs := validator.Struct(&req); errs != nil {
			response.GinValidationError(c, errs)
			return
		}

		slugName := str.Slugify(req.Name)
		tempPassword := str.Random(8)

		response.GinCreated(c, gin.H{
			"slug":     slugName,
			"password": tempPassword,
			"email":    req.Email,
			"framework": "Gin",
		})
	})

	srv := &http.Server{Addr: ":3000", Handler: r}
	
	// Start Server in a goroutine
	go func() {
		logger.Info("Gin Server is starting", "port", ":3000")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Gin Server error", "err", err)
		}
	}()

	// Register Gin shutdown
	graceful.Register(func(ctx context.Context) error {
		logger.Info("Shutting down Gin server...")
		return srv.Shutdown(ctx)
	})
}

// =====================================================================
// 3. Echo REST API Example
// =====================================================================
func RunEchoAPIExample() {
	fmt.Println("\n--- 3. Echo REST API ---")
	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = middleware.EchoErrorHandler

	// Setup Middlewares
	e.Use(middleware.EchoRecover())
	e.Use(middleware.EchoTelemetry("HTTP Request"))

	// Define Routes
	e.POST("/users", func(c echo.Context) error {
		var req CreateUserRequest
		if err := c.Bind(&req); err != nil {
			return response.EchoError(c, http.StatusBadRequest, "Invalid input data")
		}

		if errs := validator.Struct(&req); errs != nil {
			return response.EchoValidationError(c, errs)
		}

		slugName := str.Slugify(req.Name)
		tempPassword := str.Random(8)

		return response.EchoCreated(c, map[string]interface{}{
			"slug":     slugName,
			"password": tempPassword,
			"email":    req.Email,
			"framework": "Echo",
		})
	})

	// Start Server in a goroutine
	go func() {
		port := ":3000"
		logger.Info("Echo Server is starting", "port", port)
		if err := e.Start(port); err != nil && err != http.ErrServerClosed {
			logger.Error("Echo Server error", "err", err)
		}
	}()

	// Register Echo shutdown
	graceful.Register(func(ctx context.Context) error {
		logger.Info("Shutting down Echo server...")
		return e.Shutdown(ctx)
	})
}
