// Package graphql provides a framework-agnostic wrapper around gqlgen.
// It includes production-ready defaults such as OpenTelemetry integration,
// structured error handling with xerrors, panic recovery, and a high-performance
// Generic DataLoader pattern to prevent N+1 database queries.
package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
)

// Config contains core configuration for the GraphQL server
type Config struct {
	EnableTelemetry bool
}

// NewServer creates a framework-agnostic GraphQL server (net/http.Handler)
func NewServer(es graphql.ExecutableSchema, cfg Config) *handler.Server {
	srv := handler.NewDefaultServer(es)

	// Register custom Error Presenter
	srv.SetErrorPresenter(ErrorPresenter)

	// Register Recover function to prevent server crash on panic
	srv.SetRecoverFunc(RecoverFunc)

	// Register Telemetry if enabled
	if cfg.EnableTelemetry {
		srv.Use(TelemetryInterceptor{})
	}

	return srv
}
