package core

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/websocket/pubsub"
)

type panicCoordinator struct {
	panics atomic.Int32
}

func (c *panicCoordinator) GetShardID(string) string { return "recovery-test" }
func (c *panicCoordinator) GetPubSubManager() pubsub.Manager {
	return pubsub.NewNoopManager()
}
func (c *panicCoordinator) DecrTotalConnections() {
	if c.panics.Add(1) == 1 {
		panic("synthetic coordinator panic")
	}
}

func TestShardRunRestartsAfterOperationPanic(t *testing.T) {
	coordinator := &panicCoordinator{}
	shard := NewShard("recovery-test", coordinator)

	done := make(chan struct{})
	go func() {
		shard.Run()
		close(done)
	}()

	first := NewConnection(newMockConn(), shard, "user-1", shard.name, "127.0.0.1", "node-1", "request-1")
	shard.activeConns[first] = true
	shard.userSessions[first.userID] = map[*Connection]bool{first: true}
	atomic.StoreInt64(&shard.connectionCount, 1)

	// The coordinator panic occurs after the selected unregister operation is
	// consumed. The loop must restart without replaying it or retaining the lock.
	shard.unregister <- first
	second := NewConnection(newMockConn(), shard, "user-2", shard.name, "127.0.0.2", "node-1", "request-2")
	shard.register <- second

	deadline := time.After(time.Second)
	for shard.GetConnectionCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("shard did not process registration after recovered panic")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	shard.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shard did not stop after cancellation")
	}
}
