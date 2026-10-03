package health

import (
	"github.com/gofiber/fiber/v2"
	"net/http"
)

func statusCode(r HealthResponse) int {
	if r.Status == StatusOK {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

// FiberHandler handles GET /health — runs all checks.
func FiberHandler(c *Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckHealth(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}

// FiberReadyHandler handles GET /ready — runs readiness checks only.
func FiberReadyHandler(c *Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckReadiness(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}

// FiberLiveHandler handles GET /live — runs liveness checks only.
func FiberLiveHandler(c *Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckLiveness(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}
