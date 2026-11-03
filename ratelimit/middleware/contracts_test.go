package middleware_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	echolimit "github.com/thanhbvha/go-common/v2/ratelimit/middleware/echo"
	fiberlimit "github.com/thanhbvha/go-common/v2/ratelimit/middleware/fiber"
	ginlimit "github.com/thanhbvha/go-common/v2/ratelimit/middleware/gin"
	nethttplimit "github.com/thanhbvha/go-common/v2/ratelimit/middleware/nethttp"
)

type limiter struct {
	result *ratelimit.Result
	err    error
}

func (l limiter) Allow(context.Context, string, ratelimit.Config) (*ratelimit.Result, error) {
	return l.result, l.err
}

type adapter func(ratelimit.Limiter, ratelimit.MiddlewareConfig) (*http.Response, error)

func TestAdapterFailureAndQuotaContracts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adapters := map[string]adapter{
		"nethttp": func(l ratelimit.Limiter, cfg ratelimit.MiddlewareConfig) (*http.Response, error) {
			mw, err := nethttplimit.Middleware(l, cfg, func(*http.Request) string { return "client" })
			if err != nil {
				return nil, err
			}
			rec := httptest.NewRecorder()
			mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
			return rec.Result(), nil
		},
		"gin": func(l ratelimit.Limiter, cfg ratelimit.MiddlewareConfig) (*http.Response, error) {
			mw, err := ginlimit.Middleware(l, cfg, func(*gin.Context) string { return "client" })
			if err != nil {
				return nil, err
			}
			router := gin.New()
			router.Use(mw)
			router.GET("/", func(c *gin.Context) { c.Status(204) })
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
			return rec.Result(), nil
		},
		"echo": func(l ratelimit.Limiter, cfg ratelimit.MiddlewareConfig) (*http.Response, error) {
			mw, err := echolimit.Middleware(l, cfg, func(echo.Context) string { return "client" })
			if err != nil {
				return nil, err
			}
			router := echo.New()
			router.Use(mw)
			router.GET("/", func(c echo.Context) error { return c.NoContent(204) })
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
			return rec.Result(), nil
		},
		"fiber": func(l ratelimit.Limiter, cfg ratelimit.MiddlewareConfig) (*http.Response, error) {
			mw, err := fiberlimit.Middleware(l, cfg, func(*fiber.Ctx) string { return "client" })
			if err != nil {
				return nil, err
			}
			router := fiber.New()
			router.Use(mw)
			router.Get("/", func(c *fiber.Ctx) error { return c.SendStatus(204) })
			return router.Test(httptest.NewRequest("GET", "/", nil))
		},
	}
	for name, serve := range adapters {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				value    limiter
				policy   ratelimit.FailurePolicy
				status   int
				observed bool
			}{
				{"allow", limiter{result: &ratelimit.Result{Allowed: true, Remaining: 1}}, ratelimit.FailClosed, 204, false},
				{"quota", limiter{result: &ratelimit.Result{ResetAfter: 1500 * time.Millisecond}}, ratelimit.FailClosed, 429, false},
				{"outage_closed", limiter{err: errors.New("offline")}, ratelimit.FailClosed, 503, true},
				{"outage_open", limiter{err: errors.New("offline")}, ratelimit.FailOpen, 204, true},
				{"missing_result", limiter{}, ratelimit.FailClosed, 503, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					observed := false
					cfg := ratelimit.MiddlewareConfig{Policy: ratelimit.Config{Limit: 1, Window: time.Second}, FailurePolicy: tc.policy, OnError: func(context.Context, error) { observed = true }}
					response, err := serve(tc.value, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					if response.StatusCode != tc.status || observed != tc.observed {
						t.Fatalf("status=%d observed=%v", response.StatusCode, observed)
					}
					if tc.status == 429 && response.Header.Get("Retry-After") != "2" {
						t.Fatalf("Retry-After=%s", response.Header.Get("Retry-After"))
					}
				})
			}
			if _, err := serve(limiter{}, ratelimit.MiddlewareConfig{Policy: ratelimit.Config{Limit: 1, Window: time.Second}}); err == nil {
				t.Fatal("implicit failure policy accepted")
			}
		})
	}
}
