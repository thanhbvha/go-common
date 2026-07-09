package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/ratelimit"
)

func main() {
	fmt.Println("=== RateLimit Module Example ===")

	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Assuming a local Redis is running
	})

	// 2. Initialize the Rate Limiter
	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit")

	// 3. Configure the Limit: Max 5 requests per 10 seconds
	cfg := ratelimit.Config{
		Limit:  5,
		Window: 10 * time.Second,
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// 4. Apply RateLimit Middleware to all routes under /api
	// We use the default Key Generator which uses the Client IP as the rate limit key.
	api := app.Group("/api", ratelimit.FiberMiddleware(limiter, cfg, nil))

	api.Get("/data", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "Success! You have not exceeded the rate limit.",
		})
	})

	// 5. Start the server
	fmt.Println("Server is running on http://localhost:3000")
	fmt.Println("Try to quickly refresh http://localhost:3000/api/data more than 5 times!")
	log.Fatal(app.Listen(":3000"))
}
