package adapter_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	echoadapter "github.com/thanhbvha/go-common/graphql/adapter/echo"
	fiberadapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
	ginadapter "github.com/thanhbvha/go-common/graphql/adapter/gin"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"go.opentelemetry.io/otel/trace"
)

type loaderKey struct{}
type loaderIdentity struct{ id int }

func TestAdapterContextContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stack := range []string{"nethttp", "gin", "echo", "fiber"} {
		for _, setup := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/setup_%v", stack, setup), func(t *testing.T) {
				traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
				spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
				base := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
				base = ctxkey.SetUserID(base, "authenticated-user")
				base = ctxkey.SetRequestID(base, "request-id")
				deadline := time.Now().Add(time.Minute)
				base, cancel := context.WithDeadline(base, deadline)
				defer cancel()
				var seen []*loaderIdentity
				core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx := r.Context()
					if id, _ := ctxkey.GetUserID(ctx); id != "authenticated-user" {
						t.Error("identity lost")
					}
					if id, _ := ctxkey.GetRequestID(ctx); id != "request-id" {
						t.Error("request ID lost")
					}
					if trace.SpanContextFromContext(ctx).TraceID() != traceID {
						t.Error("trace lost")
					}
					if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) {
						t.Error("deadline lost")
					}
					loader, _ := ctx.Value(loaderKey{}).(*loaderIdentity)
					if setup && loader == nil {
						t.Error("setup context lost")
					}
					if loader != nil {
						seen = append(seen, loader)
					}
					w.WriteHeader(204)
				})
				inject := func(ctx context.Context) context.Context {
					return context.WithValue(ctx, loaderKey{}, &loaderIdentity{id: len(seen)})
				}
				var h http.Handler
				var app *fiber.App
				switch stack {
				case "nethttp":
					h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						ctx := r.Context()
						if setup {
							ctx = inject(ctx)
						}
						core.ServeHTTP(w, r.WithContext(ctx))
					})
				case "gin":
					router := gin.New()
					cfg := ginadapter.Config{}
					if setup {
						cfg.ContextSetup = func(ctx context.Context, _ *gin.Context) context.Context { return inject(ctx) }
					}
					router.Any("/query", ginadapter.NewHandler(core, cfg))
					h = router
				case "echo":
					router := echo.New()
					cfg := echoadapter.Config{}
					if setup {
						cfg.ContextSetup = func(ctx context.Context, _ echo.Context) context.Context { return inject(ctx) }
					}
					router.Any("/query", echoadapter.NewHandler(core, cfg))
					h = router
				case "fiber":
					app = fiber.New()
					app.Use(func(c *fiber.Ctx) error { c.SetUserContext(base); return c.Next() })
					cfg := fiberadapter.Config{}
					if setup {
						cfg.ContextSetup = func(ctx context.Context, _ *fiber.Ctx) context.Context { return inject(ctx) }
					}
					app.All("/query", fiberadapter.NewHandler(core, cfg))
				}
				for i := 0; i < 2; i++ {
					req := httptest.NewRequest("POST", "/query", nil).WithContext(base)
					if app != nil {
						res, err := app.Test(req)
						if err != nil {
							t.Fatal(err)
						}
						res.Body.Close()
						if res.StatusCode != 204 {
							t.Fatal(res.StatusCode)
						}
					} else {
						w := httptest.NewRecorder()
						h.ServeHTTP(w, req)
						if w.Code != 204 {
							t.Fatal(w.Code)
						}
					}
				}
				if setup && (len(seen) != 2 || seen[0] == seen[1]) {
					t.Fatal("request-scoped values were reused")
				}
			})
		}
	}
}
