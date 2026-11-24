package core

import (
	"context"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
	natspubsub "github.com/thanhbvha/go-common/v2/websocket/pubsub/nats"
	redispubsub "github.com/thanhbvha/go-common/v2/websocket/pubsub/redis"
)

func TestBroadcastGlobalDeliversOnceInStandalone(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() pubsub.Manager
	}{
		{"redis", func() pubsub.Manager { return redispubsub.NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone) }},
		{"nats", func() pubsub.Manager { return natspubsub.NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := tc.open()
			t.Cleanup(func() { _ = bus.Shutdown() })
			manager := NewManagerWithConfig(ManagerConfig{PubSub: bus, PubSubOwnership: PubSubBorrowed, DisableLifecycleLogging: true})
			t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
			shard := manager.GetOrCreateShard(defaultShardName)
			connection := NewConnection(newMockConn(), shard, "user", shard.name, "", manager.nodeID, "")
			if !manager.reserveConnection() || !manager.registerReservedConnection(shard, connection) {
				t.Fatal("could not register local connection")
			}

			const payload = `{"message":"hello"}`
			shard.BroadcastGlobal([]byte(payload))
			// Shutdown waits for accepted loopback callbacks, so both paths are settled.
			if err := bus.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if got := len(connection.send); got != 1 {
				t.Fatalf("local deliveries = %d, want exactly 1", got)
			}
			if got := <-connection.send; string(got) != payload {
				t.Fatalf("local payload = %q, want %q", got, payload)
			}
		})
	}
}
