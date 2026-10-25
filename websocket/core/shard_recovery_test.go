package core

import (
	"sync/atomic"
	"testing"

	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

type panicCoordinator struct {
	panics atomic.Int32
}

func (c *panicCoordinator) GetShardID(string) string { return "recovery-test" }
func (c *panicCoordinator) GetPubSubManager() pubsub.Manager {
	return pubsub.NewNoopManager()
}
func (c *panicCoordinator) GetEventExecutor() *pool.PartitionedExecutor {
	return pool.NewPartitionedExecutor(1, 16)
}
func (c *panicCoordinator) DecrTotalConnections() {
	if c.panics.Add(1) == 1 {
		panic("synthetic coordinator panic")
	}
}

func TestShardLockStateRemainsUsableAfterCoordinatorPanic(t *testing.T) {
	coordinator := &panicCoordinator{}
	shard := NewShard("recovery-test", coordinator)

	first := NewConnection(newMockConn(), shard, "user-1", shard.name, "127.0.0.1", "node-1", "request-1")
	shard.activeConns[first] = struct{}{}
	shard.userSessions[first.userID] = map[*Connection]struct{}{first: {}}
	atomic.StoreInt64(&shard.connectionCount, 1)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected coordinator panic")
			}
		}()
		shard.Unregister(first)
	}()

	second := NewConnection(newMockConn(), shard, "user-2", shard.name, "127.0.0.2", "node-1", "request-2")
	if !shard.Register(second) || shard.GetConnectionCount() != 1 {
		t.Fatal("shard lock state was not usable after panic")
	}
}
