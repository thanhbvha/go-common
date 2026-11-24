package fiber

import (
	"context"
	"net/http"
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
