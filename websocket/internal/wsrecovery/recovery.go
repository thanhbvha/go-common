// Package wsrecovery centralizes structured panic reporting for WebSocket
// runtime goroutines. HTTP upgrade panics remain the responsibility of the
// framework-specific web recovery middleware.
package wsrecovery

import (
	"context"
	"log/slog"

	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

// Metadata identifies the WebSocket operation in which a panic occurred.
type Metadata struct {
	Component   string
	Operation   string
	TaskType    string
	EventType   string
	MessageType string
	Channel     string
	NodeID      string
	UserID      string
	ClientIP    string
	RequestID   string
	ShardID     string
	RoomID      string
}

// Handler creates a recovery callback that logs a bounded structured report.
// It must be passed directly to a deferred recovery.Catch call in the goroutine
// being protected.
func Handler(metadata Metadata) panicrecovery.Handler {
	return func(report panicrecovery.Report) {
		ctx := context.Background()
		if metadata.RequestID != "" {
			ctx = ctxkey.SetRequestID(ctx, metadata.RequestID)
		}
		logger.ErrorWithContext(ctx, "websocket panic recovered",
			slog.String("component", metadata.Component),
			slog.String("operation", metadata.Operation),
			slog.String("task_type", metadata.TaskType),
			slog.String("event_type", metadata.EventType),
			slog.String("message_type", metadata.MessageType),
			slog.String("channel", metadata.Channel),
			slog.String("node_id", metadata.NodeID),
			slog.String("panic_type", report.Type),
			slog.String("panic_message", report.Message),
			slog.Any("panic_cause", report.Cause),
			slog.String("file", report.Origin.File),
			slog.Int("line", report.Origin.Line),
			slog.String("function", report.Origin.Function),
			slog.Any("stack", report.Stack),
			slog.String("user_id", metadata.UserID),
			slog.String("client_ip", metadata.ClientIP),
			slog.String("shard_id", metadata.ShardID),
			slog.String("room_id", metadata.RoomID),
		)
	}
}
