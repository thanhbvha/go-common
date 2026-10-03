package ginhealth

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/health"
)

func statusCode(r health.HealthResponse) int {
	if r.Status == health.StatusOK {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

// Handler handles GET /health for Gin.
func Handler(c *health.Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckHealth(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}

// ReadyHandler handles GET /ready for Gin.
func ReadyHandler(c *health.Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckReadiness(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}

// LiveHandler handles GET /live for Gin.
func LiveHandler(c *health.Checker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		r := c.CheckLiveness(ctx.Request.Context())
		ctx.JSON(statusCode(r), r)
	}
}
