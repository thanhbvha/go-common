// Package fiber provides an adapter to bridge the Fiber web framework and our generic WebSocket core.
package fiber

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/gofiber/websocket/v2"
)

// MiddlewareManager handles standard HTTP middlewares for securing and throttling WebSocket endpoints.
type MiddlewareManager struct {
	app               *fiber.App
	rateLimitMax      int
	rateLimitDuration time.Duration
}

// NewMiddlewareManager instantiates a MiddlewareManager for the specified Fiber app with custom limit thresholds.
func NewMiddlewareManager(app *fiber.App, rateLimitMax int, rateLimitDuration time.Duration) *MiddlewareManager {
	if rateLimitMax <= 0 {
		rateLimitMax = 100
	}
	if rateLimitDuration <= 0 {
		rateLimitDuration = time.Minute
	}

	return &MiddlewareManager{
		app:               app,
		rateLimitMax:      rateLimitMax,
		rateLimitDuration: rateLimitDuration,
	}
}

// SetupMiddlewares registers recovery, CORS, helmet, request ID, logger, and rate limiters on the Fiber application.
func (m *MiddlewareManager) SetupMiddlewares() {
	// Standard upgrade verification on WebSocket entry path
	m.app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	m.app.Use(helmet.New())

	m.app.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin,Content-Type,Accept,Authorization,X-Requested-With",
		AllowCredentials: false,
		MaxAge:           86400,
	}))

	m.app.Use(recover.New())
	m.app.Use(requestid.New())
	m.app.Use(logger.New())

	m.app.Use(limiter.New(limiter.Config{
		Max:        m.rateLimitMax,
		Expiration: m.rateLimitDuration,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Rate limit exceeded",
			})
		},
	}))
}
