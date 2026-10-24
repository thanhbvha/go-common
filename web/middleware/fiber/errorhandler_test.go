package fibermw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestErrorHandlerMasksInternalFiberError(t *testing.T) {
	const secret = "database password is secret"
	app := fiber.New(fiber.Config{ErrorHandler: ErrorHandler})
	app.Get("/error", func(*fiber.Ctx) error {
		return fiber.NewError(http.StatusInternalServerError, secret)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	message, _ := body["message"].(string)
	if strings.Contains(message, secret) {
		t.Fatalf("response exposed internal error detail: %q", message)
	}
}
