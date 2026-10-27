package echoauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

type testClaims struct{ ID string }

type testValidator struct{}

func (testValidator) ValidateRequestToken(context.Context, string) (*testClaims, error) {
	return &testClaims{ID: "user-1"}, nil
}

func TestGenericMiddlewareStoresClaims(t *testing.T) {
	e := echo.New()
	e.GET("/profile", func(c echo.Context) error {
		local, localOK := c.Get(string(ctxkey.UserInfo)).(*testClaims)
		value, contextOK := ctxkey.GetUserInfo(c.Request().Context())
		fromContext, contextTyped := value.(*testClaims)
		if !localOK || !contextOK || !contextTyped || local.ID != "user-1" || fromContext.ID != "user-1" {
			t.Fatalf("echo context = %#v, request context = %#v", local, value)
		}
		return c.NoContent(http.StatusNoContent)
	}, GenericMiddleware[*testClaims](testValidator{}))

	req := httptest.NewRequest(http.MethodGet, "/profile", nil)
	req.Header.Set("Authorization", "Bearer token")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}
