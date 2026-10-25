package core

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

type routingPubSub struct {
	mu            sync.RWMutex
	handlers      map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)
	registrations map[pubsub.HandlerRegistration]string
	shutdowns     int
}

func newRoutingPubSub() *routingPubSub {
	return &routingPubSub{handlers: make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)), registrations: make(map[pubsub.HandlerRegistration]string)}
}
func (p *routingPubSub) RegisterHandler(messageType string, handler func(*pubsub.CrossNodeMessage)) pubsub.HandlerRegistration {
	registration := pubsub.NewHandlerRegistration()
	p.mu.Lock()
	if p.handlers[messageType] == nil {
		p.handlers[messageType] = make(map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage))
	}
	p.handlers[messageType][registration] = handler
	p.registrations[registration] = messageType
	p.mu.Unlock()
	return registration
}
func (p *routingPubSub) UnregisterHandler(registration pubsub.HandlerRegistration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	messageType, ok := p.registrations[registration]
	if !ok {
		return
	}
	delete(p.registrations, registration)
	delete(p.handlers[messageType], registration)
}
func (*routingPubSub) Subscribe(...string) error                             { return nil }
func (*routingPubSub) Unsubscribe(...string) error                           { return nil }
func (*routingPubSub) Publish(string, *pubsub.CrossNodeMessage) error        { return nil }
func (*routingPubSub) BroadcastMessage(string, map[string]interface{}) error { return nil }
func (*routingPubSub) BroadcastUserNotification(string, string, map[string]interface{}) error {
	return nil
}
func (*routingPubSub) BroadcastRoomMessage(string, string, map[string]interface{}) error { return nil }
func (*routingPubSub) BroadcastChatMessage(string, string, map[string]interface{}) error { return nil }
func (*routingPubSub) NotifyUserJoin(string, string, string) error                       { return nil }
func (*routingPubSub) NotifyUserLeave(string, string) error                              { return nil }
func (*routingPubSub) NotifyShardCreate(string) error                                    { return nil }
func (*routingPubSub) NotifyShardDestroy(string) error                                   { return nil }
func (*routingPubSub) SendNodeStatus(map[string]interface{}) error                       { return nil }
func (*routingPubSub) GetNodeID() string                                                 { return "routing-node" }
func (*routingPubSub) GetStats() map[string]interface{}                                  { return nil }
func (p *routingPubSub) Shutdown() error {
	p.mu.Lock()
	p.shutdowns++
	p.mu.Unlock()
	return nil
}

func (p *routingPubSub) dispatch(message *pubsub.CrossNodeMessage) {
	p.mu.RLock()
	handlers := make([]func(*pubsub.CrossNodeMessage), 0, len(p.handlers[message.Type]))
	for _, handler := range p.handlers[message.Type] {
		handlers = append(handlers, handler)
	}
	p.mu.RUnlock()
	for _, handler := range handlers {
		handler(message)
	}
}

func TestManagerRoutesPubSubAcrossOneHundredShards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bus := newRoutingPubSub()
	manager := &Manager{
		shards: make(map[string]*Shard), ctx: ctx, cancel: cancel, nodeID: bus.GetNodeID(),
		pubsubManager: bus, eventExecutor: pool.NewPartitionedExecutor(2, 32),
	}
	manager.state.Store(int32(StateRunning))
	t.Cleanup(func() { cancel(); _ = manager.eventExecutor.Shutdown(context.Background()) })
	manager.setupPubSubHandlers()

	var target *Connection
	for i := 0; i < 100; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		shard := NewShard(shardID, manager)
		manager.shards[shardID] = shard
		transport := newMockConn()
		connection := NewConnection(transport, shard, "user", shardID, "127.0.0.1", manager.nodeID, "request")
		if !manager.reserveConnection() || !manager.registerReservedConnection(shard, connection) {
			t.Fatal("failed to register test connection")
		}
		if i == 0 {
			target = connection
		}
	}

	bus.dispatch(&pubsub.CrossNodeMessage{
		Type: pubsub.MessageTypeNotification, ShardID: "shard-0", UserID: "user",
		Data: map[string]interface{}{"message": "routed"},
	})
	if target == nil {
		t.Fatal("target connection missing")
	}
	select {
	case payload := <-target.send:
		if string(payload) != "routed" {
			t.Fatalf("payload = %q", payload)
		}
	default:
		t.Fatal("target shard did not receive routed message")
	}
}

func TestManagerShutdownReleasesOnlyItsBorrowedPubSubHandlers(t *testing.T) {
	bus := newRoutingPubSub()
	first := NewManagerWithConfig(ManagerConfig{PubSub: bus, PubSubOwnership: PubSubBorrowed})
	second := NewManagerWithConfig(ManagerConfig{PubSub: bus, PubSubOwnership: PubSubBorrowed})

	bus.mu.RLock()
	before := len(bus.registrations)
	bus.mu.RUnlock()
	if before != 16 {
		t.Fatalf("registrations before shutdown = %d", before)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	bus.mu.RLock()
	afterFirst := len(bus.registrations)
	shutdowns := bus.shutdowns
	bus.mu.RUnlock()
	if afterFirst != 8 || shutdowns != 0 {
		t.Fatalf("registrations=%d shutdowns=%d", afterFirst, shutdowns)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerShutdownClosesOwnedPubSub(t *testing.T) {
	bus := newRoutingPubSub()
	manager := NewManagerWithConfig(ManagerConfig{PubSub: bus, PubSubOwnership: PubSubOwned})
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	bus.mu.RLock()
	shutdowns := bus.shutdowns
	bus.mu.RUnlock()
	if shutdowns != 1 {
		t.Fatalf("shutdowns = %d", shutdowns)
	}
}
