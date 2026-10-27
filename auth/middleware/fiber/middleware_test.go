package fiberauth

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

type testClaims struct{ ID string }

type testValidator struct{}

func (testValidator) ValidateRequestToken(context.Context, string) (*testClaims, error) {
	return &testClaims{ID: "user-1"}, nil
}

func TestGenericMiddlewareStoresClaims(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/profile", GenericMiddleware[*testClaims](testValidator{}), func(c *fiber.Ctx) error {
		local, localOK := c.Locals(string(ctxkey.UserInfo)).(*testClaims)
		value, contextOK := ctxkey.GetUserInfo(c.UserContext())
		fromContext, typed := value.(*testClaims)
		if !localOK || !contextOK || !typed || local.ID != "user-1" || fromContext.ID != "user-1" {
			t.Fatalf("locals = %#v, context = %#v", local, value)
		}
		return c.SendStatus(fiber.StatusNoContent)
	})

	req := httptest.NewRequest("GET", "/profile", nil)
	req.Header.Set("Authorization", "Bearer token")
	response, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != fiber.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}
}
