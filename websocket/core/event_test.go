package core

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventHandlerContinuesAfterPanic(t *testing.T) {
	eventType := fmt.Sprintf("panic-test-%d", time.Now().UnixNano())
	var calls atomic.Int32
	done := make(chan struct{})
	RegisterHandler(eventType, func(_ *Connection, _ IncomingMessage) error {
		if calls.Add(1) == 1 {
			panic("synthetic event panic")
		}
		close(done)
		return nil
	})

	transport := newMockConn()
	connection := NewConnection(transport, &Shard{name: "shard-test"}, "user-test", "shard-test", "127.0.0.1", "node-test", "request-test")
	t.Cleanup(func() { connection.Close("test cleanup") })
	raw := []byte(fmt.Sprintf(`{"type":%q,"data":{"secret":"panic-diagnostic-must-not-be-sent"}}`, eventType))

	HandleEvent(&EventMessage{Conn: connection, Raw: raw})
	time.Sleep(20 * time.Millisecond)
	HandleEvent(&EventMessage{Conn: connection, Raw: raw})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event handler did not continue after panic")
	}

	select {
	case frame := <-transport.writeChan:
		t.Fatalf("panic diagnostics were written to client frame: %q", frame)
	default:
	}
}
