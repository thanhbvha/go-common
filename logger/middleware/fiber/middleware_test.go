package fiberlog

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

func TestRequestIDMiddlewarePopulatesLocalAndContext(t *testing.T) {
	app := fiber.New()
	app.Use(RequestIDMiddleware())
	app.Get("/", func(c *fiber.Ctx) error {
		contextID, ok := ctxkey.GetRequestID(c.UserContext())
		localID, localOK := c.Locals("request_id").(string)
		if !ok || !localOK || contextID == "" || contextID != localID {
			t.Fatalf("context ID = %q, local ID = %q", contextID, localID)
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	response, err := app.Test(httptest.NewRequest("GET", "/", nil), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.Header.Get("X-Request-ID") == "" {
		t.Fatal("response is missing X-Request-ID")
	}
}

func TestRequestIDMiddlewareReplacesInvalidID(t *testing.T) {
	app := fiber.New()
	app.Use(RequestIDMiddleware())
	app.Get("/", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) })
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Request-ID", strings.Repeat("a", 129))
	response, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := response.Header.Get("X-Request-ID"); got == req.Header.Get("X-Request-ID") || got == "" {
		t.Fatalf("invalid request ID was not replaced: %q", got)
	}
}
