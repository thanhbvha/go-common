# RateLimit Module

The `ratelimit` module provides an ultra-fast, distributed Rate Limiting solution using Redis and Lua scripting. It is designed to protect your APIs against DDoS, Brute-force attacks, and API abuse, while keeping the limit synchronized across a cluster of microservices.

## Features

1. **Atomic Operations**: Uses Redis Lua scripts to evaluate and increment rate limits in a single, atomic operation to prevent race conditions in highly concurrent environments.
2. **Standard Algorithm**: Implements the Fixed Window rate limiting algorithm.
3. **Framework-Agnostic Middlewares**: Ready-to-use middlewares for `Fiber`, `Gin`, and `Echo`.
4. **Customizable Keys**: You can rate-limit based on Client IP (default), User ID, API Key, or any custom logic.
5. **Standard Headers**: Automatically injects standard Rate Limit HTTP Headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`) into responses.

## Installation

```bash
go get github.com/thanhbvha/go-common/ratelimit
```

## Basic Usage (Fiber)

```go
package main

import (
	"time"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/ratelimit"
)

func main() {
	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// 2. Initialize the Limiter
	limiter := ratelimit.NewRedisLimiter(rdb, "api_limit")

	// 3. Setup Limit: Max 10 requests per 1 minute
	cfg := ratelimit.Config{
		Limit:  10,
		Window: 1 * time.Minute,
	}

	app := fiber.New()

	// 4. Apply Middleware (Uses IP by default)
	app.Use("/api", ratelimit.FiberMiddleware(limiter, cfg, nil))

	app.Get("/api/data", func(c *fiber.Ctx) error {
		return c.SendString("Success!")
	})

	app.Listen(":3000")
}
```

## Advanced Usage: Custom Rate Limit Keys

By default, the middleware limits requests by IP address. In many systems, you want to limit by Authenticated User ID or API Key instead. You can do this by passing a custom Key Generator function.

```go
// Rate limit by User ID (assuming auth middleware sets it in context)
keyGen := func(c *fiber.Ctx) string {
	userID := c.Locals("user_id")
	if userID != nil {
		return userID.(string)
	}
	// Fallback to IP if not logged in
	return c.IP()
}

app.Use("/api", ratelimit.FiberMiddleware(limiter, cfg, keyGen))
```
