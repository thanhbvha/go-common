package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/health"
	echohealth "github.com/thanhbvha/go-common/health/handler/echo"
	fiberhealth "github.com/thanhbvha/go-common/health/handler/fiber"
	ginhealth "github.com/thanhbvha/go-common/health/handler/gin"
	nethttphealth "github.com/thanhbvha/go-common/health/handler/nethttp"
	"github.com/thanhbvha/go-common/utils/graceful"
)

const address = ":3000"

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()
	checker := newChecker()

	switch *stack {
	case "nethttp":
		runNetHTTP(checker)
	case "fiber":
		runFiber(checker)
	case "gin":
		runGin(checker)
	case "echo":
		runEcho(checker)
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
}

func newChecker() *health.Checker {
	checker := health.New().WithVersion("v1.2.0").WithTimeout(2 * time.Second)
	checker.AddReadinessCheck("postgresql", delayedCheck(100*time.Millisecond, nil))
	checker.AddReadinessCheck("redis", delayedCheck(50*time.Millisecond, errors.New("dependency unavailable")))
	checker.AddReadinessCheck("mongodb", delayedCheck(150*time.Millisecond, nil))
	checker.AddLivenessCheck("memory", func(context.Context) error { return nil })
	return checker
}

func delayedCheck(delay time.Duration, result error) health.CheckFunc {
	return func(ctx context.Context) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return result
		}
	}
}

func runNetHTTP(checker *health.Checker) {
	mux := http.NewServeMux()
	mux.Handle("/health", nethttphealth.Handler(checker))
	mux.Handle("/ready", nethttphealth.ReadyHandler(checker))
	mux.Handle("/live", nethttphealth.LiveHandler(checker))
	runHTTPServer("net/http", &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second})
}

func runFiber(checker *health.Checker) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/health", fiberhealth.Handler(checker))
	app.Get("/ready", fiberhealth.ReadyHandler(checker))
	app.Get("/live", fiberhealth.LiveHandler(checker))
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(context.Context) error { return app.Shutdown() })
	go func() { _ = app.Listen(address) }()
	fmt.Printf("Fiber health example listening on http://localhost%s\n", address)
	shutdown.Wait()
}

func runGin(checker *health.Checker) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/health", ginhealth.Handler(checker))
	router.GET("/ready", ginhealth.ReadyHandler(checker))
	router.GET("/live", ginhealth.LiveHandler(checker))
	runHTTPServer("Gin", &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second})
}

func runEcho(checker *health.Checker) {
	e := echo.New()
	e.HideBanner = true
	e.GET("/health", echohealth.Handler(checker))
	e.GET("/ready", echohealth.ReadyHandler(checker))
	e.GET("/live", echohealth.LiveHandler(checker))
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return e.Shutdown(ctx) })
	go func() { _ = e.Start(address) }()
	fmt.Printf("Echo health example listening on http://localhost%s\n", address)
	shutdown.Wait()
}

func runHTTPServer(name string, server *http.Server) {
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
	go func() { _ = server.ListenAndServe() }()
	fmt.Printf("%s health example listening on http://localhost%s\n", name, address)
	shutdown.Wait()
}
