package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/thanhbvha/go-common/foundation"
	foundationhttp "github.com/thanhbvha/go-common/foundation/adapter/nethttp"
	healthhttp "github.com/thanhbvha/go-common/health/handler/nethttp"
)

func runAPI(ctx context.Context) error {
	app, err := foundation.New(foundation.Config{
		Name:            "foundation-api",
		Version:         "v1.0.0",
		StartupTimeout:  10 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /health", healthhttp.Handler(app.Health()))
	mux.Handle("GET /ready", healthhttp.ReadyHandler(app.Health()))
	mux.Handle("GET /live", healthhttp.LiveHandler(app.Health()))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "hello from foundation")
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := app.Register(foundationhttp.Component("http", server)); err != nil {
		return err
	}
	return app.Run(ctx)
}
