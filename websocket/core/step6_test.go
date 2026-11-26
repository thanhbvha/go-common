package core

import (
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandleEventRejectsNonUTF8(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	shard := manager.GetOrCreateShard(defaultShardName)
	connection := NewConnection(newMockConn(), shard, "user", shard.name, "", manager.nodeID, "")

	HandleEvent(&EventMessage{Conn: connection, Raw: []byte("{\"type\":\"x\",\"data\":\"\xff\"}"), queuedAt: time.Now()})
	if got := connection.malformedMessages.Load(); got != 1 {
		t.Fatalf("malformed = %d, want 1", got)
	}
}

func TestDefaultSlowConsumerThresholdToleratesBursts(t *testing.T) {
	if got := DefaultManagerConfig().SlowConsumerDropThreshold; got < 2 {
		t.Fatalf("SlowConsumerDropThreshold = %d, want > 1", got)
	}
}

func TestRepeatedViolationsCloseWithPolicyFrame(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	shard.config.MaxViolations = 3

	bad := &EventMessage{Conn: connection, Raw: []byte("not json"), queuedAt: time.Now()}
	for range 2 {
		HandleEvent(bad)
	}
	if connection.IsClosed() {
		t.Fatal("closed before reaching MaxViolations")
	}
	HandleEvent(bad)
	waitConnection(t, connection)
	if got := transport.closeCode(); got != websocket.ClosePolicyViolation {
		t.Fatalf("close code = %d, want %d", got, websocket.ClosePolicyViolation)
	}
}

func TestSlowConsumerCloseCodeIsTryAgainLater(t *testing.T) {
	transport := newMockConn()
	connection, _, _ := newRunningTestConnection(t, transport)
	connection.requestClose(websocket.CloseTryAgainLater, "test")
	waitConnection(t, connection)
	if got := transport.closeCode(); got != websocket.CloseTryAgainLater {
		t.Fatalf("close code = %d, want %d", got, websocket.CloseTryAgainLater)
	}
}

// closeCode returns the code of the close frame the writer sent, or -1.
func (m *mockConn) closeCode() int {
	for {
		select {
		case f := <-m.writes:
			if f.messageType == websocket.CloseMessage && len(f.data) >= 2 {
				return int(f.data[0])<<8 | int(f.data[1])
			}
		default:
			return -1
		}
	}
}

func TestDisconnectIdentitySendsPolicyFrame(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	connection.setIdentity(Identity{UserID: "u"})
	manager := &Manager{shards: map[string]*Shard{shard.name: shard}}
	if n := manager.DisconnectIdentity(func(Identity) bool { return true }, "revoked"); n != 1 {
		t.Fatalf("disconnected = %d, want 1", n)
	}
	waitConnection(t, connection)
	if got := transport.closeCode(); got != websocket.ClosePolicyViolation {
		t.Fatalf("close code = %d, want %d", got, websocket.ClosePolicyViolation)
	}
}

func TestRejectBeforePumpsSendsCloseFrame(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	shard := manager.GetOrCreateShard(defaultShardName)
	transport := newMockConn()
	connection := NewConnection(transport, shard, "u", shard.name, "", manager.nodeID, "")
	connection.rejectAndWait(websocket.CloseTryAgainLater, "full")
	if got := transport.closeCode(); got != websocket.CloseTryAgainLater {
		t.Fatalf("close code = %d, want %d", got, websocket.CloseTryAgainLater)
	}
	if transport.closeCount.Load() != 1 {
		t.Fatalf("socket closed %d times, want 1", transport.closeCount.Load())
	}
}

func TestShardAtCapacityRejectsWithTryAgainLater(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{DisableLifecycleLogging: true, MaxShardConnections: 1})
	t.Cleanup(func() { _ = manager.Shutdown(t.Context()) })
	shard := manager.GetOrCreateShard(defaultShardName)
	first := NewConnection(newMockConn(), shard, "a", shard.name, "", manager.nodeID, "")
	if !shard.Register(first) {
		t.Fatal("first register failed")
	}
	transport := newMockConn()
	second := NewConnection(transport, shard, "b", shard.name, "", manager.nodeID, "")
	if shard.Register(second) {
		t.Fatal("second register must fail")
	}
	<-second.closedDone
	if got := transport.closeCode(); got != websocket.CloseTryAgainLater {
		t.Fatalf("close code = %d, want %d", got, websocket.CloseTryAgainLater)
	}
}

func TestViolationSendsRateLimitedErrorEnvelope(t *testing.T) {
	transport := newMockConn()
	connection, _, _ := newRunningTestConnection(t, transport)
	t.Cleanup(func() { connection.Close("test complete"); waitConnection(t, connection) })
	bad := &EventMessage{Conn: connection, Raw: []byte("not json"), queuedAt: time.Now()}
	for range 5 {
		HandleEvent(bad)
	}
	deadline := time.After(time.Second)
	envelopes := 0
	for envelopes == 0 {
		select {
		case f := <-transport.writes:
			if f.messageType == websocket.TextMessage && strings.Contains(string(f.data), `"code":"malformed"`) {
				envelopes++
			}
		case <-deadline:
			t.Fatal("no error envelope sent")
		}
	}
	time.Sleep(50 * time.Millisecond)
	for len(transport.writes) > 0 {
		if f := <-transport.writes; strings.Contains(string(f.data), `"type":"error"`) {
			t.Fatal("more than one envelope within a second")
		}
	}
}
