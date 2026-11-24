package core

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandleEventRejectsNonUTF8(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	shard := manager.GetOrCreateShard(defaultShardName)
	connection := NewConnection(newMockConn(), shard, "user", shard.name, "", manager.nodeID, "")

	HandleEvent(&EventMessage{Conn: connection, Raw: []byte("{\"type\":\"x\",\"data\":\"\xff\"}"), queuedAt: time.Now()})
	if got := connection.malformedMessages.Load(); got != 1 {
		t.Fatalf("malformed = %d, want 1", got)
	}
}

func TestDefaultSlowConsumerThresholdToleratesBursts(t *testing.T) {
	if got := DefaultManagerConfig().SlowConsumerDropThreshold; got < 2 {
		t.Fatalf("SlowConsumerDropThreshold = %d, want > 1", got)
	}
}

func TestRepeatedViolationsCloseWithPolicyFrame(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	shard.config.MaxViolations = 3

	bad := &EventMessage{Conn: connection, Raw: []byte("not json"), queuedAt: time.Now()}
	for range 2 {
		HandleEvent(bad)
	}
	if connection.IsClosed() {
		t.Fatal("closed before reaching MaxViolations")
	}
	HandleEvent(bad)
	waitConnection(t, connection)
	if got := transport.closeCode(); got != websocket.ClosePolicyViolation {
		t.Fatalf("close code = %d, want %d", got, websocket.ClosePolicyViolation)
	}
}

func TestSlowConsumerCloseCodeIsTryAgainLater(t *testing.T) {
	transport := newMockConn()
	connection, _, _ := newRunningTestConnection(t, transport)
	connection.requestClose(websocket.CloseTryAgainLater, "test")
	waitConnection(t, connection)
	if got := transport.closeCode(); got != websocket.CloseTryAgainLater {
		t.Fatalf("close code = %d, want %d", got, websocket.CloseTryAgainLater)
	}
}

// closeCode returns the code of the close frame the writer sent, or -1.
func (m *mockConn) closeCode() int {
	for {
		select {
		case f := <-m.writes:
			if f.messageType == websocket.CloseMessage && len(f.data) >= 2 {
				return int(f.data[0])<<8 | int(f.data[1])
			}
		default:
			return -1
		}
	}
}
