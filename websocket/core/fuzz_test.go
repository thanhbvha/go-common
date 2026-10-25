package core

import (
	"context"
	"testing"

	"github.com/thanhbvha/go-common/websocket/pool"
)

func FuzzIncomingEnvelope(f *testing.F) {
	f.Add([]byte(`{"type":"chat","data":null}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		coordinator := &lifecycleCoordinator{executor: pool.NewPartitionedExecutor(1, 8)}
		defer coordinator.executor.Shutdown(context.Background())
		shard := NewShard("fuzz", coordinator)
		connection := NewConnection(newMockConn(), shard, "user", shard.name, "127.0.0.1", "node", "request")
		HandleEvent(&EventMessage{Conn: connection, Raw: raw})
		connection.Close("fuzz complete")
	})
}

func FuzzConnectionLifecycle(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, operations []byte) {
		coordinator := &lifecycleCoordinator{executor: pool.NewPartitionedExecutor(1, 8)}
		defer coordinator.executor.Shutdown(context.Background())
		shard := NewShard("fuzz", coordinator)
		connection := NewConnection(newMockConn(), shard, "user", shard.name, "127.0.0.1", "node", "request")
		shard.Register(connection)
		for _, operation := range operations {
			switch operation % 5 {
			case 0:
				connection.Send([]byte("message"))
			case 1:
				_ = shard.JoinRoom(context.Background(), "room", connection)
			case 2:
				_ = shard.LeaveRoom(context.Background(), "room", connection)
			case 3:
				shard.removeConnection(connection, "fuzz unregister")
			case 4:
				connection.Close("fuzz close")
			}
		}
		connection.Close("fuzz complete")
		shard.removeConnection(connection, "fuzz complete")
	})
}
