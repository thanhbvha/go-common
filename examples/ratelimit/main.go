package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/ratelimit"
)

func main() {
	fmt.Println("=== RateLimit Module Examples ===")
	
	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Assuming a local Redis is running
	})
	
	// Uncomment the example you want to run:
	RunIPBasedRateLimitExample(rdb)
	// RunUserIDBasedRateLimitExample(rdb)
	// RunGinRateLimitExample(rdb)
	// RunEchoRateLimitExample(rdb)
}

// =====================================================================
// 1. IP-Based Rate Limiting (Fiber - Default)
// =====================================================================
func RunIPBasedRateLimitExample(rdb *redis.Client) {
	fmt.Println("\n--- 1. IP-Based Rate Limiting (Fiber) ---")

	// Initialize the Rate Limiter
	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit_ip")

	// Configure the Limit: Max 5 requests per 10 seconds
	cfg := ratelimit.Config{
		Limit:  5,
		Window: 10 * time.Second,
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// Apply RateLimit Middleware to all routes under /api
	// By default (nil KeyGenerator), the middleware uses Client IP as the rate limit key.
	api := app.Group("/api", ratelimit.FiberMiddleware(limiter, cfg, nil))

	api.Get("/data", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "Success! You have not exceeded the rate limit.",
		})
	})

	// Start the server
	fmt.Println("Server is running on http://localhost:3000")
	fmt.Println("Try to quickly refresh http://localhost:3000/api/data more than 5 times!")
	log.Fatal(app.Listen(":3000"))
}

// =====================================================================
// 2. User-ID Based Rate Limiting (Fiber - Custom KeyGenerator)
// =====================================================================
func RunUserIDBasedRateLimitExample(rdb *redis.Client) {
	fmt.Println("\n--- 2. User-ID Based Rate Limiting (Fiber) ---")

	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit_user")

	cfg := ratelimit.Config{
		Limit:  3,
		Window: 10 * time.Second,
	}

	// CRITICAL: In production, ALWAYS pass a custom KeyGenerator function
	// that extracts the User ID or API Token to prevent IP spoofing or NAT issues.
	keyGenerator := func(c *fiber.Ctx) string {
		// Example: Get user_id from header (in reality, from JWT token context)
		userID := c.Get("X-User-Id")
		if userID == "" {
			return "anonymous" // Fallback key
		}
		return userID
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	api := app.Group("/api", ratelimit.FiberMiddleware(limiter, cfg, keyGenerator))

	api.Get("/data", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "Success! You have not exceeded your personal rate limit.",
		})
	})

	fmt.Println("Server is running on http://localhost:3000")
	fmt.Println("To test:")
	fmt.Println("  curl -H \"X-User-Id: User1\" http://localhost:3000/api/data")
	fmt.Println("  curl -H \"X-User-Id: User2\" http://localhost:3000/api/data")
	log.Fatal(app.Listen(":3000"))
}

// =====================================================================
// 3. Gin Framework Example
// =====================================================================
func RunGinRateLimitExample(rdb *redis.Client) {
	fmt.Println("\n--- 3. Gin Framework Rate Limiting ---")

	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit_gin")
	cfg := ratelimit.Config{Limit: 5, Window: 10 * time.Second}

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Apply GinMiddleware
	api := r.Group("/api", ratelimit.GinMiddleware(limiter, cfg, nil))

	api.GET("/data", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Success! (Gin)",
		})
	})

	fmt.Println("Gin server is running on http://localhost:3000")
	log.Fatal(r.Run(":3000"))
}

// =====================================================================
// 4. Echo Framework Example
// =====================================================================
func RunEchoRateLimitExample(rdb *redis.Client) {
	fmt.Println("\n--- 4. Echo Framework Rate Limiting ---")

	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit_echo")
	cfg := ratelimit.Config{Limit: 5, Window: 10 * time.Second}

	e := echo.New()
	e.HideBanner = true

	// Apply EchoMiddleware
	api := e.Group("/api", ratelimit.EchoMiddleware(limiter, cfg, nil))

	api.GET("/data", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{
			"message": "Success! (Echo)",
		})
	})

	fmt.Println("Echo server is running on http://localhost:3000")
	log.Fatal(e.Start(":3000"))
}
