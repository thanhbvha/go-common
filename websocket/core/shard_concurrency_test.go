package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestShardConcurrentUnregisterAndSendToUser(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			shard.SendToUser(connection.userID, []byte("notification"))
		}
	}()
	go func() {
		defer wg.Done()
		shard.removeConnection(connection, "concurrent unregister")
	}()
	wg.Wait()
	connection.Close("test complete")
	waitConnection(t, connection)
	if shard.GetConnectionCount() != 0 {
		t.Fatalf("shard connection count = %d", shard.GetConnectionCount())
	}
}

func TestShardRoomJoinLeaveAndFanOut(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	ctx := context.Background()

	if err := shard.JoinRoom(ctx, "room-1", connection); err != nil {
		t.Fatal(err)
	}
	if err := shard.JoinRoom(ctx, "room-1", connection); err != nil {
		t.Fatal(err)
	}
	if size := shard.GetRoomSize("room-1"); size != 1 {
		t.Fatalf("room size after idempotent join = %d", size)
	}

	shard.SendToRoom("room-1", []byte("room-message"))
	select {
	case frame := <-transport.writes:
		if string(frame.data) != "room-message" {
			t.Fatalf("room frame = %q", frame.data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for room message")
	}

	if err := shard.LeaveRoom(ctx, "room-1", connection); err != nil {
		t.Fatal(err)
	}
	if err := shard.LeaveRoom(ctx, "room-1", connection); err != nil {
		t.Fatal(err)
	}
	if size := shard.GetRoomSize("room-1"); size != 0 {
		t.Fatalf("room size after idempotent leave = %d", size)
	}

	connection.Close("test complete")
	waitConnection(t, connection)
}

func TestShardConcurrentRoomLeaveAndSend(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	if err := shard.JoinRoom(context.Background(), "room-1", connection); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			shard.SendToRoom("room-1", []byte("message"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = shard.LeaveRoom(context.Background(), "room-1", connection)
			_ = shard.JoinRoom(context.Background(), "room-1", connection)
		}
	}()
	wg.Wait()

	connection.Close("test complete")
	waitConnection(t, connection)
	if size := shard.GetRoomSize("room-1"); size != 0 {
		t.Fatalf("unregister left %d room members", size)
	}
}

func TestShardUnregisterOnlyTouchesJoinedRooms(t *testing.T) {
	firstTransport := newMockConn()
	first, shard, _ := newRunningTestConnection(t, firstTransport)
	second := NewConnection(newMockConn(), shard, "user-2", shard.name, "127.0.0.2", "node-1", "req-2")
	if !shard.Register(second) {
		t.Fatal("failed to register second connection")
	}
	if err := shard.JoinRoom(context.Background(), "first-room", first); err != nil {
		t.Fatal(err)
	}
	if err := shard.JoinRoom(context.Background(), "second-room", second); err != nil {
		t.Fatal(err)
	}

	shard.removeConnection(first, "test cleanup")
	if size := shard.GetRoomSize("first-room"); size != 0 {
		t.Fatalf("first room size = %d", size)
	}
	if size := shard.GetRoomSize("second-room"); size != 1 {
		t.Fatalf("unrelated room size = %d", size)
	}
	shard.removeConnection(second, "test cleanup")
}

func TestShardJoinRoomRejectsInactiveConnection(t *testing.T) {
	coordinator := &lifecycleCoordinator{}
	shard := NewShard("test-shard", coordinator)
	connection := NewConnection(newMockConn(), shard, "user-1", shard.name, "127.0.0.1", "node-1", "request-1")
	if err := shard.JoinRoom(context.Background(), "room-1", connection); err != ErrConnectionInactive {
		t.Fatalf("JoinRoom error = %v", err)
	}
}

func TestShardShutdownRemovesActiveConnectionsOnce(t *testing.T) {
	transport := newMockConn()
	connection, shard, coordinator := newRunningTestConnection(t, transport)
	if err := shard.JoinRoom(context.Background(), "room-1", connection); err != nil {
		t.Fatal(err)
	}

	_ = shard.Shutdown(context.Background())
	_ = shard.Shutdown(context.Background())
	waitConnection(t, connection)
	if shard.GetConnectionCount() != 0 || shard.GetRoomSize("room-1") != 0 {
		t.Fatalf("shard count=%d room size=%d", shard.GetConnectionCount(), shard.GetRoomSize("room-1"))
	}
	if coordinator.decrements.Load() != 1 {
		t.Fatalf("manager decrements = %d", coordinator.decrements.Load())
	}
}
