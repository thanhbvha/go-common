package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerCapacityReservationIsAtomic(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	atomic.StoreInt64(&manager.totalConnections, maxTotalConnections-1)

	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if manager.reserveConnection() {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted reservations = %d", accepted.Load())
	}
	if manager.GetTotalConnections() != maxTotalConnections-1 || manager.reservedConnections != 1 {
		t.Fatalf("active=%d reserved=%d", manager.GetTotalConnections(), manager.reservedConnections)
	}
	manager.rollbackConnection()
	if manager.GetTotalConnections() != maxTotalConnections-1 || manager.reservedConnections != 0 {
		t.Fatalf("rollback left active=%d reserved=%d", manager.GetTotalConnections(), manager.reservedConnections)
	}
	atomic.StoreInt64(&manager.totalConnections, 0)
	for i := 0; i < 100; i++ {
		manager.DecrTotalConnections()
	}
	if manager.GetTotalConnections() != 0 {
		t.Fatalf("connection count went negative: %d", manager.GetTotalConnections())
	}
}

func TestManagerImmediateDisconnectRollsAccountingBack(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	transport := newMockConn()
	transport.readDeadlineErr = errSyntheticDeadline

	if err := manager.HandleConnection(transport, defaultShardName, "user-1", "127.0.0.1", "request-1"); err != nil {
		t.Fatal(err)
	}
	shard, ok := manager.GetShard(defaultShardName)
	if !ok {
		t.Fatal("default shard missing")
	}
	if manager.GetTotalConnections() != 0 || shard.GetConnectionCount() != 0 {
		t.Fatalf("manager total=%d shard total=%d", manager.GetTotalConnections(), shard.GetConnectionCount())
	}
}

func TestManagerRegistrationFailureRollsReservationBack(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	shard, ok := manager.GetShard(defaultShardName)
	if !ok {
		t.Fatal("default shard missing")
	}
	_ = shard.Shutdown(context.Background())

	transport := newMockConn()
	if err := manager.HandleConnection(transport, defaultShardName, "user-1", "127.0.0.1", "request-1"); err == nil {
		t.Fatal("expected registration failure")
	}
	manager.accountingMutex.Lock()
	reserved := manager.reservedConnections
	manager.accountingMutex.Unlock()
	if manager.GetTotalConnections() != 0 || reserved != 0 {
		t.Fatalf("active=%d reserved=%d", manager.GetTotalConnections(), reserved)
	}
}

func TestManagerRejectsConnectionsAfterShutdown(t *testing.T) {
	manager := NewManager()
	_ = manager.Shutdown(context.Background())

	transport := newMockConn()
	if err := manager.HandleConnection(transport, "new-shard", "user-1", "127.0.0.1", "request-1"); err == nil {
		t.Fatal("expected shutdown manager to reject connection")
	}
	if manager.GetTotalConnections() != 0 {
		t.Fatalf("active connections = %d", manager.GetTotalConnections())
	}
	if _, exists := manager.GetShard("new-shard"); exists {
		t.Fatal("created shard after manager shutdown")
	}
}

func TestManagerRegistrationConcurrentWithShardShutdownKeepsCountsEqual(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })

	for i := 0; i < 100; i++ {
		shard := NewShard("race-shard", manager)
		connection := NewConnection(newMockConn(), shard, "user-race", shard.name, "127.0.0.1", manager.nodeID, "request-race")
		if !manager.reserveConnection() {
			t.Fatal("failed to reserve connection")
		}

		var registered bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			registered = manager.registerReservedConnection(shard, connection)
		}()
		go func() {
			defer wg.Done()
			_ = shard.Shutdown(context.Background())
		}()
		wg.Wait()
		if registered {
			connection.Close("test complete")
		}
		if manager.GetTotalConnections() != shard.GetConnectionCount() {
			t.Fatalf("iteration %d: manager total=%d shard total=%d", i, manager.GetTotalConnections(), shard.GetConnectionCount())
		}
	}
}

func TestManagerCountMatchesShardCounts(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	transports := []*mockConn{newMockConn(), newMockConn(), newMockConn()}

	var wg sync.WaitGroup
	for i, transport := range transports {
		wg.Add(1)
		go func(i int, transport *mockConn) {
			defer wg.Done()
			_ = manager.HandleConnection(transport, defaultShardName, "user-count", "127.0.0.1", "request-count")
		}(i, transport)
	}

	waitForCondition(t, func() bool {
		stats := manager.GetStats()
		return stats["totalConnections"].(int64) == int64(len(transports)) &&
			stats["shardConnections"].(int64) == int64(len(transports))
	})
	stats := manager.GetStats()
	if stats["totalConnections"] != stats["shardConnections"] {
		t.Fatalf("manager total=%v shard total=%v", stats["totalConnections"], stats["shardConnections"])
	}

	for _, transport := range transports {
		_ = transport.Close()
	}
	wg.Wait()
	waitForCondition(t, func() bool { return manager.GetTotalConnections() == 0 })
}

var errSyntheticDeadline = &syntheticError{"deadline failed"}

type syntheticError struct{ message string }

func (e *syntheticError) Error() string { return e.message }

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
