package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/ratelimit"
	echolimit "github.com/thanhbvha/go-common/ratelimit/middleware/echo"
	fiberlimit "github.com/thanhbvha/go-common/ratelimit/middleware/fiber"
	ginlimit "github.com/thanhbvha/go-common/ratelimit/middleware/gin"
	nethttplimit "github.com/thanhbvha/go-common/ratelimit/middleware/nethttp"
	"github.com/thanhbvha/go-common/utils/graceful"
)

const address = ":3000"

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: envOrDefault("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Redis is required for this example: %v\n", err)
		os.Exit(1)
	}

	limiter := ratelimit.NewRedisLimiter(rdb, "example_rate_limit_"+*stack)
	cfg := ratelimit.Config{Limit: 5, Window: 10 * time.Second}
	switch *stack {
	case "nethttp":
		runNetHTTP(limiter, cfg)
	case "fiber":
		runFiber(limiter, cfg)
	case "gin":
		runGin(limiter, cfg)
	case "echo":
		runEcho(limiter, cfg)
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
}

func runNetHTTP(limiter ratelimit.Limiter, cfg ratelimit.Config) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/data", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"request allowed","stack":"net/http"}`))
	})
	handler := nethttplimit.Middleware(limiter, cfg, nil)(mux)
	runHTTPServer("net/http", &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second})
}

func runFiber(limiter ratelimit.Limiter, cfg ratelimit.Config) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/api/data", fiberlimit.Middleware(limiter, cfg, nil), func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"message": "request allowed", "stack": "fiber"})
	})
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(context.Context) error { return app.Shutdown() })
	go func() { _ = app.Listen(address) }()
	fmt.Printf("Fiber rate-limit example listening on http://localhost%s\n", address)
	shutdown.Wait()
}

func runGin(limiter ratelimit.Limiter, cfg ratelimit.Config) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/api/data", ginlimit.Middleware(limiter, cfg, nil), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "request allowed", "stack": "gin"})
	})
	runHTTPServer("Gin", &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second})
}

func runEcho(limiter ratelimit.Limiter, cfg ratelimit.Config) {
	e := echo.New()
	e.HideBanner = true
	e.GET("/api/data", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"message": "request allowed", "stack": "echo"})
	}, echolimit.Middleware(limiter, cfg, nil))
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return e.Shutdown(ctx) })
	go func() { _ = e.Start(address) }()
	fmt.Printf("Echo rate-limit example listening on http://localhost%s\n", address)
	shutdown.Wait()
}

func runHTTPServer(name string, server *http.Server) {
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
	go func() { _ = server.ListenAndServe() }()
	fmt.Printf("%s rate-limit example listening on http://localhost%s\n", name, address)
	shutdown.Wait()
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
