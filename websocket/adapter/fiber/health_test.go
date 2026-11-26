package fiber

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func probe(t *testing.T, app *fiberframework.App, path string) int {
	t.Helper()
	response, err := app.Test(fiberTestRequest("http://example.com" + path))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestServerRoutesSeparateLivenessFromReadiness(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	server := NewServer(8080, NewHandler(Config{Manager: manager}))
	server.SetupRoutes()
	app := server.App()

	for _, path := range []string{"/livez", "/readyz", "/health"} {
		if got := probe(t, app, path); got != http.StatusOK {
			t.Fatalf("running node: %s = %d", path, got)
		}
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := probe(t, app, "/livez"); got != http.StatusOK {
		t.Fatalf("a stopped node must stay live so it is not restarted mid-drain, got %d", got)
	}
	for _, path := range []string{"/readyz", "/health"} {
		if got := probe(t, app, path); got != http.StatusServiceUnavailable {
			t.Fatalf("a stopped node must not be ready: %s = %d", path, got)
		}
	}
}

func TestAdminEndpointsRequireToken(t *testing.T) {
	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	do := func(token, header, path string) int {
		h := NewHandler(Config{Manager: manager, AdminToken: token})
		app := fiberframework.New()
		app.Get("/stats", h.HandleStats)
		app.Get("/shard", h.HandleShardManagement)
		req := fiberTestRequest("http://example.com" + path)
		req.Header.Set("Authorization", header)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	for _, path := range []string{"/stats", "/shard?shard=x"} {
		if got := do("", "Bearer ", path); got != http.StatusUnauthorized {
			t.Fatalf("%s no token configured: %d", path, got)
		}
		if got := do("s3cret", "Bearer wrong", path); got != http.StatusUnauthorized {
			t.Fatalf("%s wrong token: %d", path, got)
		}
	}
	if got := do("s3cret", "Bearer s3cret", "/stats"); got != http.StatusOK {
		t.Fatalf("valid token: %d", got)
	}
}

func TestStartReportsBusyPortAndServesWhenBound(t *testing.T) {
	probeListener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := probeListener.Addr().(*net.TCPAddr).Port

	busy := NewServer(port, NewHandler(Config{}))
	if err := busy.Start(); err == nil {
		t.Fatal("Start on an occupied port must fail immediately")
	}
	_ = probeListener.Close()

	manager := core.NewManagerWithConfig(core.DefaultManagerConfig())
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	server := NewServer(port, NewHandler(Config{Manager: manager}))
	server.SetupRoutes()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown() })
	resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/livez")
	if err != nil {
		t.Fatalf("listener not accepting right after Start returned: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
