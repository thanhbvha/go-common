package wsrecovery

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
)

func TestHandlerLogsStructuredPanicAndConnectionMetadata(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "websocket-recovery.log")
	previous := logger.Default()
	testLogger := logger.New(logger.Options{
		File: &logger.FileOptions{Path: logPath},
	})
	logger.SetDefault(testLogger)
	t.Cleanup(func() {
		logger.SetDefault(previous)
		testLogger.Close()
	})

	Handler(Metadata{
		Component:   "adapter/test.connection",
		Operation:   "dispatch",
		TaskType:    "worker-task",
		EventType:   "chat_message",
		MessageType: "broadcast",
		Channel:     "shard:shard-1",
		NodeID:      "node-1",
		UserID:      "user-1",
		ClientIP:    "127.0.0.1",
		RequestID:   "request-1",
		ShardID:     "shard-1",
		RoomID:      "room-1",
	})(panicrecovery.Report{
		Message: "synthetic panic",
		Type:    "string",
		Cause:   errors.New("wrapped failure"),
		Origin: panicrecovery.Frame{
			File:     "handler.go",
			Line:     42,
			Function: "example.handler",
		},
		Stack: []panicrecovery.Frame{{File: "handler.go", Line: 42, Function: "example.handler"}},
	})

	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := string(contents)
	for _, want := range []string{
		"websocket panic recovered",
		"adapter/test.connection",
		"dispatch",
		"worker-task",
		"chat_message",
		"broadcast",
		"shard:shard-1",
		"node-1",
		"synthetic panic",
		"wrapped failure",
		"handler.go",
		"example.handler",
		"user-1",
		"127.0.0.1",
		"request-1",
		"shard-1",
		"room-1",
	} {
		if !strings.Contains(entry, want) {
			t.Fatalf("log entry does not contain %q: %s", want, entry)
		}
	}
}
