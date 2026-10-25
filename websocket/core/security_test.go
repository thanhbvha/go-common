package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type denyAuthorizer struct{}

func (denyAuthorizer) CanSubscribe(context.Context, Identity, string) error { return ErrUnauthorized }
func (denyAuthorizer) CanPublish(context.Context, Identity, string) error   { return ErrUnauthorized }

func TestSecurityRejectsUnauthorizedRoomAndEvent(t *testing.T) {
	SetAuthorizer(denyAuthorizer{})
	t.Cleanup(func() { SetAuthorizer(nil) })
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	if err := shard.JoinRoom(context.Background(), "room", connection); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("JoinRoom error = %v", err)
	}
	eventType := fmt.Sprintf("denied-%d", time.Now().UnixNano())
	var calls atomic.Int32
	RegisterHandler(eventType, func(*Connection, IncomingMessage) error { calls.Add(1); return nil })
	HandleEvent(&EventMessage{Conn: connection, Raw: []byte(fmt.Sprintf(`{"type":%q,"data":{}}`, eventType))})
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("unauthorized handler executed")
	}
	connection.Close("test complete")
	waitConnection(t, connection)
}

func TestSecurityHandlerPayloadValidation(t *testing.T) {
	transport := newMockConn()
	connection, _, _ := newRunningTestConnection(t, transport)
	eventType := fmt.Sprintf("validated-%d", time.Now().UnixNano())
	var calls atomic.Int32
	RegisterHandlerConfig(eventType, HandlerConfig{
		MaxPayloadBytes: 4,
		Validate: func(data []byte) error {
			if string(data) != `null` {
				return ErrInvalidPayload
			}
			return nil
		},
		Handler: func(context.Context, *Connection, IncomingMessage) error { calls.Add(1); return nil },
	})
	HandleEvent(&EventMessage{Conn: connection, Raw: []byte(fmt.Sprintf(`{"type":%q,"data":{"large":true}}`, eventType))})
	HandleEvent(&EventMessage{Conn: connection, Raw: []byte(fmt.Sprintf(`{"type":%q,"data":1}`, eventType))})
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("invalid handler calls = %d", calls.Load())
	}
	connection.Close("test complete")
	waitConnection(t, connection)
}

func TestSecurityRoomLimitAndRevocation(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	for i := 0; i < 100; i++ {
		if err := shard.JoinRoom(context.Background(), fmt.Sprintf("room-%d", i), connection); err != nil {
			t.Fatal(err)
		}
	}
	if err := shard.JoinRoom(context.Background(), "overflow", connection); !errors.Is(err, ErrRoomLimit) {
		t.Fatalf("room limit error = %v", err)
	}
	connection.Close("revoked")
	waitConnection(t, connection)
}

func TestManagerDisconnectIdentity(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	transport := newMockConn()
	done := make(chan error, 1)
	go func() {
		done <- manager.HandleIdentityConnection(transport, defaultShardName, Identity{UserID: "user", SessionID: "revoked"}, "127.0.0.1", "request")
	}()
	waitForCondition(t, func() bool { return manager.GetTotalConnections() == 1 })
	if count := manager.DisconnectIdentity(func(identity Identity) bool { return identity.SessionID == "revoked" }, "session revoked"); count != 1 {
		t.Fatalf("disconnected = %d", count)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("revoked connection did not finish")
	}
}
