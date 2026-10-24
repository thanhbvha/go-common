package fiber

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/websocket/limiter"
)

func TestDefaultOriginPolicyRejectsCrossOrigin(t *testing.T) {
	handler := NewHandler(Config{
		Authenticate:      func(*fiberframework.Ctx) (string, error) { return "user", nil },
		RateLimiter:       limiter.NewRateLimiter(100, 100),
		ConnectionLimiter: limiter.NewConnectionLimiter(1),
	})
	app := fiberframework.New()
	app.Get("/ws", handler.HandleUpgrade)
	req := websocketUpgradeRequest("http://example.com/ws")
	req.Host = "example.com"
	req.Header.Set("Origin", "https://evil.example")

	response, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestDefaultAuthenticationRejectsUpgrade(t *testing.T) {
	handler := NewHandler(Config{})
	app := fiberframework.New()
	app.Get("/ws", handler.HandleUpgrade)
	response, err := app.Test(websocketUpgradeRequest("http://example.com/ws?token=unverified"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestGetRequestIDPrefersSharedContext(t *testing.T) {
	app := fiberframework.New()
	var got string
	app.Use(func(c *fiberframework.Ctx) error {
		c.Locals("request_id", "local-id")
		c.SetUserContext(ctxkey.SetRequestID(c.UserContext(), "context-id"))
		return c.Next()
	})
	app.Get("/", func(c *fiberframework.Ctx) error {
		got = getRequestID(c)
		return c.SendStatus(http.StatusNoContent)
	})
	response, err := app.Test(httptest.NewRequest(http.MethodGet, "http://example.com/", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got != "context-id" {
		t.Fatalf("request ID = %q", got)
	}
}

func TestGetRequestIDIgnoresLegacyFiberLocal(t *testing.T) {
	app := fiberframework.New()
	var got string
	app.Use(func(c *fiberframework.Ctx) error {
		c.Locals("requestid", "legacy-id")
		return c.Next()
	})
	app.Get("/", func(c *fiberframework.Ctx) error {
		got = getRequestID(c)
		return c.SendStatus(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	req.Header.Set("X-Request-ID", "header-id")
	response, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got != "header-id" {
		t.Fatalf("request ID = %q, want header-id", got)
	}
}

func TestServerDefaultsDoNotTrustAllProxiesAndMaskInternalErrors(t *testing.T) {
	handler := NewHandler(Config{Authenticate: func(*fiberframework.Ctx) (string, error) {
		return "", errors.New("unused")
	}})
	server := NewServer(8080, handler)
	config := server.App().Config()
	if config.EnableTrustedProxyCheck || len(config.TrustedProxies) != 0 || config.ProxyHeader != "" {
		t.Fatalf("unsafe proxy defaults: check=%v proxies=%v header=%q", config.EnableTrustedProxyCheck, config.TrustedProxies, config.ProxyHeader)
	}
	server.App().Get("/error", func(*fiberframework.Ctx) error {
		return errors.New("database password is secret")
	})
	response, err := server.App().Test(httptest.NewRequest(http.MethodGet, "http://example.com/error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "password") {
		t.Fatalf("internal error leaked: %s", body)
	}
}

func websocketUpgradeRequest(target string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return req
}
