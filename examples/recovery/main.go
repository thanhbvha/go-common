package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/logger"
	fiberlog "github.com/thanhbvha/go-common/logger/middleware/fiber"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/utils/graceful"
	fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
)

func main() {
	applicationLogger := logger.New(logger.DefaultOptions())
	logger.SetDefault(applicationLogger)
	defer logger.Close()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	shutdown := graceful.NewShutdown(10 * time.Second)
	// Recovery is first so it also protects middleware registered after it.
	app.Use(fibermw.Recover())
	app.Use(fiberlog.RequestIDMiddleware())

	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"panic_route":      "GET /panic",
			"background_route": "POST /background",
		})
	})

	app.Get("/panic", func(*fiber.Ctx) error {
		panic("synthetic handler panic")
	})

	app.Post("/background", func(c *fiber.Ctx) error {
		requestID, _ := c.Locals("request_id").(string)
		ctx := context.Background()
		if requestID != "" {
			ctx = context.WithValue(ctx, logger.ContextKeyRequestID, requestID)
		}
		go runBackgroundJob(ctx)
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
			"status":     "accepted",
			"request_id": requestID,
		})
	})

	go func() {
		logger.Info("recovery example is listening", "address", "http://127.0.0.1:3000")
		if err := app.Listen("127.0.0.1:3000"); err != nil {
			logger.Error("recovery example stopped", "err", err)
		}
	}()

	shutdown.Register(func(ctx context.Context) error {
		return app.ShutdownWithContext(ctx)
	})
	shutdown.Wait()
}

func runBackgroundJob(ctx context.Context) {
	defer panicrecovery.Catch(func(report panicrecovery.Report) {
		logger.ErrorWithContext(ctx, "background panic recovered",
			slog.String("panic_type", report.Type),
			slog.String("panic_message", report.Message),
			slog.String("file", report.Origin.File),
			slog.Int("line", report.Origin.Line),
			slog.String("function", report.Origin.Function),
			slog.Any("stack", report.Stack),
		)
	})

	panic("synthetic background panic")
}
