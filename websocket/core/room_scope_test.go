package core

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

// These are characterization tests for the legacy room API. They pin down the
// documented limitation (rooms are shard scoped) so a behaviour change is a
// deliberate decision rather than an accident. Clusters must use
// routing.Gateway + routing.Router, which key rooms by tenant and room.

// twoShardRoom builds one node whose users live on different shards, as they do
// in production because adapters pick the shard with hash(userID). Both users
// have joined the same room.
func twoShardRoom(t *testing.T) (bus *routingPubSub, alice, bob *Connection) {
	t.Helper()
	bus = newRoutingPubSub()
	manager := NewManagerWithConfig(ManagerConfig{PubSub: bus, PubSubOwnership: PubSubBorrowed})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	join := func(shardID, user string) *Connection {
		shard := manager.GetOrCreateShard(shardID)
		connection := NewConnection(newMockConn(), shard, user, shardID, "127.0.0.1", manager.nodeID, "req-"+user)
		if !manager.reserveConnection() || !manager.registerReservedConnection(shard, connection) {
			t.Fatalf("could not register %s", user)
		}
		if err := shard.JoinRoom(context.Background(), "lobby", connection); err != nil {
			t.Fatalf("%s could not join: %v", user, err)
		}
		return connection
	}
	return bus, join("shard-a", "alice"), join("shard-b", "bob")
}

func received(connection *Connection) (string, bool) {
	select {
	case payload := <-connection.send:
		return string(payload), true
	case <-time.After(50 * time.Millisecond):
		return "", false
	}
}

func TestShardRoomIsScopedToItsShard(t *testing.T) {
	_, alice, bob := twoShardRoom(t)

	alice.shard.SendToRoom("lobby", []byte("from-alice"))

	if got, ok := received(alice); !ok || got != "from-alice" {
		t.Fatalf("same-shard member got %q, %v", got, ok)
	}
	if got, ok := received(bob); ok {
		t.Fatalf("a room spanning shards unexpectedly delivered across them: %q", got)
	}
	if size := alice.shard.GetRoomSize("lobby"); size != 1 {
		t.Fatalf("shard-a room size = %d, want 1: membership is per shard", size)
	}
	if size := bob.shard.GetRoomSize("lobby"); size != 1 {
		t.Fatalf("shard-b room size = %d, want 1: membership is per shard", size)
	}
}

// A room message published by a peer is routed by the sender's shard name, so
// it reaches only members on the same-named local shard.
func TestPubSubRoomMessageReachesOnlyTheNamedShard(t *testing.T) {
	bus, alice, bob := twoShardRoom(t)

	bus.dispatch(&pubsub.CrossNodeMessage{
		Type: pubsub.MessageTypeChatRoom, ShardID: "shard-a", RoomID: "lobby",
		Data: map[string]interface{}{"message": "from-peer"},
	})

	if got, ok := received(alice); !ok || got != "from-peer" {
		t.Fatalf("member on the named shard got %q, %v", got, ok)
	}
	if got, ok := received(bob); ok {
		t.Fatalf("member of the same room on another shard received %q", got)
	}
}
