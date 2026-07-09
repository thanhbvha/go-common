// Package graphql provides a framework-agnostic wrapper around gqlgen.
// It includes production-ready defaults such as OpenTelemetry integration,
// structured error handling with xerrors, panic recovery, and a high-performance
// Generic DataLoader pattern to prevent N+1 database queries.
package graphql

import (
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
)

// Config chứa các cấu hình core cho GraphQL server
type Config struct {
	EnableTelemetry bool
}

// NewServer tạo một framework-agnostic GraphQL server (net/http.Handler)
func NewServer(es graphql.ExecutableSchema, cfg Config) *handler.Server {
	srv := handler.NewDefaultServer(es)

	// Đăng ký Error Presenter tùy chỉnh
	srv.SetErrorPresenter(ErrorPresenter)

	// Đăng ký tính năng Recover để tránh sập server khi panic
	srv.SetRecoverFunc(RecoverFunc)

	// Đăng ký Telemetry nếu được bật
	if cfg.EnableTelemetry {
		srv.Use(TelemetryInterceptor{})
	}

	return srv
}
