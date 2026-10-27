package ginauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

type testClaims struct{ ID string }

type testValidator struct{}

func (testValidator) ValidateRequestToken(context.Context, string) (*testClaims, error) {
	return &testClaims{ID: "user-1"}, nil
}

func TestGenericMiddlewareStoresClaims(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/profile", GenericMiddleware[*testClaims](testValidator{}), func(c *gin.Context) {
		local, localOK := c.Get(string(ctxkey.UserInfo))
		localClaims, localTyped := local.(*testClaims)
		value, contextOK := ctxkey.GetUserInfo(c.Request.Context())
		fromContext, contextTyped := value.(*testClaims)
		if !localOK || !localTyped || !contextOK || !contextTyped || localClaims.ID != "user-1" || fromContext.ID != "user-1" {
			t.Fatalf("gin context = %#v, request context = %#v", local, value)
		}
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/profile", nil)
	req.Header.Set("Authorization", "Bearer token")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d", recorder.Code)
	}
}
