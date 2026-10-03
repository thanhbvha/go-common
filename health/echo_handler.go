package health

import "github.com/labstack/echo/v4"

// EchoHandler handles GET /health for Echo.
func EchoHandler(c *Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckHealth(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}

// EchoReadyHandler handles GET /ready for Echo.
func EchoReadyHandler(c *Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckReadiness(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}

// EchoLiveHandler handles GET /live for Echo.
func EchoLiveHandler(c *Checker) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		r := c.CheckLiveness(ctx.Request().Context())
		return ctx.JSON(statusCode(r), r)
	}
}
