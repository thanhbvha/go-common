package fiberhealth

import (
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/health"
)

func statusCode(r health.HealthResponse) int {
	if r.Status == health.StatusOK {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

// Handler handles GET /health — runs all checks.
func Handler(c *health.Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckHealth(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}

// ReadyHandler handles GET /ready — runs readiness checks only.
func ReadyHandler(c *health.Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckReadiness(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}

// LiveHandler handles GET /live — runs liveness checks only.
func LiveHandler(c *health.Checker) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		r := c.CheckLiveness(ctx.UserContext())
		return ctx.Status(statusCode(r)).JSON(r)
	}
}
