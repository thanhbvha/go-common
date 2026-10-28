package main

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/health"
)

func TestDemoDependencyLifecycle(t *testing.T) {
	dependency := &demoDependency{}
	if err := dependency.Check(t.Context()); err == nil {
		t.Fatal("expected disconnected dependency")
	}
	if err := dependency.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := dependency.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := dependency.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := dependency.Check(t.Context()); err == nil {
		t.Fatal("expected dependency to be closed")
	}
}

func TestFiberRouter(t *testing.T) {
	app := newFiberRouter(health.New())
	request := httptest.NewRequest("GET", "/", nil)
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !strings.Contains(string(body), `"service":"foundation-fiber-api"`) {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	if response.Header.Get("X-Request-ID") == "" {
		t.Fatal("Fiber demo must return a request ID")
	}
	if app.Config().ReadTimeout <= 0 {
		t.Fatal("Fiber demo must configure a positive ReadTimeout")
	}
}
