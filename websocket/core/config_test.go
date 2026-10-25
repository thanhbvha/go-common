package core

import (
	"context"
	"testing"
)

func TestManagerConfigControlsCapacityAndConnectionQueues(t *testing.T) {
	config := DefaultManagerConfig()
	config.MaxConnections = 1
	config.SendQueueSize = 7
	config.MaxRoomsPerConnection = 2
	manager := NewManagerWithConfig(config)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })

	if !manager.reserveConnection() || manager.reserveConnection() {
		t.Fatal("configured manager capacity was not enforced")
	}
	manager.rollbackConnection()
	shard, ok := manager.GetShard(defaultShardName)
	if !ok {
		t.Fatal("default shard missing")
	}
	connection := NewConnection(newMockConn(), shard, "user", shard.name, "127.0.0.1", manager.nodeID, "request")
	if cap(connection.send) != 7 {
		t.Fatalf("send queue capacity = %d", cap(connection.send))
	}
	if !connection.addRoom("one") || !connection.addRoom("two") || connection.addRoom("three") {
		t.Fatal("configured room limit was not enforced")
	}
}

func TestManagerConfigOwnsEventRegistryAndAuthorizer(t *testing.T) {
	registry := NewEventRegistry()
	called := make(chan struct{}, 1)
	registry.Register("isolated", HandlerConfig{Handler: func(context.Context, *Connection, IncomingMessage) error {
		called <- struct{}{}
		return nil
	}})
	manager := NewManagerWithConfig(ManagerConfig{EventRegistry: registry, Authorizer: denyAuthorizer{}})
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	shard, _ := manager.GetShard(defaultShardName)
	connection := NewConnection(newMockConn(), shard, "user", shard.name, "127.0.0.1", manager.nodeID, "request")

	HandleEvent(&EventMessage{Conn: connection, Raw: []byte(`{"type":"isolated","data":{}}`)})
	select {
	case <-called:
		t.Fatal("manager-scoped authorizer allowed event")
	default:
	}
}

func TestManagerIdentityQuotasReserveAndReleaseAtomically(t *testing.T) {
	config := DefaultManagerConfig()
	config.MaxConnectionsPerUser = 1
	config.MaxConnectionsPerAccount = 2
	config.MaxConnectionsPerTenant = 3
	manager := NewManagerWithConfig(config)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	identity := Identity{UserID: "user", AccountID: "account", TenantID: "tenant"}

	first := manager.reserveIdentityConnection(identity)
	if first == nil {
		t.Fatal("first identity reservation rejected")
	}
	if second := manager.reserveIdentityConnection(identity); second != nil {
		t.Fatal("per-user quota was not enforced")
	}
	manager.rollbackReservation(first)
	if next := manager.reserveIdentityConnection(identity); next == nil {
		t.Fatal("released identity quota was not reusable")
	} else {
		manager.rollbackReservation(next)
	}
}

func TestLifecycleLoggingCanBeDisabled(t *testing.T) {
	config := DefaultManagerConfig()
	config.DisableLifecycleLogging = true
	manager := NewManagerWithConfig(config)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	if !manager.GetConfig().DisableLifecycleLogging {
		t.Fatal("lifecycle logging was not disabled")
	}
}
