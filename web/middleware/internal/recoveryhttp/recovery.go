// Package recoveryhttp contains framework-neutral HTTP policy shared by the
// Fiber, Gin, and Echo panic-recovery adapters.
package recoveryhttp

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
)

const (
	StatusCode = http.StatusInternalServerError
	ErrorCode  = "INTERNAL_ERROR"
	Message    = "An unexpected internal error occurred"
)

// Response is the public JSON body returned after recovering a panic. It never
// includes the panic value, stack, file, or line.
type Response struct {
	Code      int    `json:"code"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// NewResponse creates the safe public response for a recovered panic.
func NewResponse(requestID string) Response {
	return Response{
		Code:      StatusCode,
		Status:    ErrorCode,
		Message:   Message,
		RequestID: requestID,
	}
}

// Log writes a recovered panic synchronously. The request context is reduced to
// context.Context before logging so framework contexts are never retained.
func Log(ctx context.Context, requestID string, report panicrecovery.Report) {
	if ctx == nil {
		ctx = context.Background()
	}
	if requestID != "" {
		ctx = context.WithValue(ctx, logger.ContextKeyRequestID, requestID)
	}
	logger.ErrorWithContext(ctx, "panic recovered",
		slog.String("panic_type", report.Type),
		slog.String("panic_message", report.Message),
		slog.String("file", report.Origin.File),
		slog.Int("line", report.Origin.Line),
		slog.String("function", report.Origin.Function),
		slog.Any("stack", report.Stack),
	)
}
