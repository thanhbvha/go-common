package echohealth

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/health"
)

func statusCode(r health.HealthResponse) int {
	if r.Status == health.StatusOK {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

// Handler handles GET /health for Echo.
func Handler(c *health.Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckHealth(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}

// ReadyHandler handles GET /ready for Echo.
func ReadyHandler(c *health.Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckReadiness(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}

// LiveHandler handles GET /live for Echo.
func LiveHandler(c *health.Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckLiveness(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}
