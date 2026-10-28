package main

import (
	"context"
	"time"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/foundation"
	foundationfiber "github.com/thanhbvha/go-common/foundation/adapter/fiber"
	"github.com/thanhbvha/go-common/health"
	fiberhealth "github.com/thanhbvha/go-common/health/handler/fiber"
	fiberlog "github.com/thanhbvha/go-common/logger/middleware/fiber"
	fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
)

const fiberBodyLimit = 1 << 20 // 1 MiB

func runFiberAPI(ctx context.Context) error {
	app, err := foundation.New(foundation.Config{
		Name:            "foundation-fiber-api",
		Version:         "v1.0.0",
		StartupTimeout:  10 * time.Second,
		ShutdownTimeout: 15 * time.Second,
	})
	if err != nil {
		return err
	}

	fiberApp := newFiberRouter(app.Health())
	if err := app.Register(foundationfiber.Component("fiber-http", ":8080", fiberApp)); err != nil {
		return err
	}
	return app.Run(ctx)
}

func newFiberRouter(checker *health.Checker) *fiberframework.App {
	app := fiberframework.New(fiberframework.Config{
		DisableStartupMessage: true,
		ErrorHandler:          fibermw.ErrorHandler,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          15 * time.Second,
		IdleTimeout:           60 * time.Second,
		BodyLimit:             fiberBodyLimit,
	})
	app.Use(fibermw.Recover(), fiberlog.RequestIDMiddleware())

	app.Get("/health", fiberhealth.Handler(checker))
	app.Get("/ready", fiberhealth.ReadyHandler(checker))
	app.Get("/live", fiberhealth.LiveHandler(checker))
	app.Get("/", func(ctx *fiberframework.Ctx) error {
		return ctx.JSON(fiberframework.Map{
			"service": "foundation-fiber-api",
			"status":  "ok",
		})
	})
	return app
}
