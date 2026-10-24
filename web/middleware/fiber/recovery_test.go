package fibermw

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestRecoverReturnsSafeResponse(t *testing.T) {
	const (
		requestID = "fiber-request"
		secret    = "database password is secret"
	)
	app := fiber.New()
	app.Use(Recover())
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("request_id", requestID)
		return c.Next()
	})
	app.Get("/panic", func(*fiber.Ctx) error { panic(secret) })

	response, err := app.Test(httptest.NewRequest("GET", "/panic", nil), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	assertSafeRecoveryBody(t, body, requestID, secret)
}

func assertSafeRecoveryBody(t *testing.T, body map[string]any, requestID, secret string) {
	t.Helper()
	if body["status"] != "INTERNAL_ERROR" || body["request_id"] != requestID {
		t.Fatalf("unexpected response: %#v", body)
	}
	if message, _ := body["message"].(string); strings.Contains(message, secret) {
		t.Fatalf("response exposed panic value: %q", message)
	}
}
