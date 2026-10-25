package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

type testFrame struct {
	messageType int
	data        []byte
}

type mockConn struct {
	reads            chan []byte
	writes           chan testFrame
	closed           chan struct{}
	closeOnce        sync.Once
	closeCount       atomic.Int32
	writeGate        chan struct{}
	readDeadlineErr  error
	writeDeadlineErr error
	pongHandler      func(string) error
	mu               sync.Mutex
}

func newMockConn() *mockConn {
	return &mockConn{
		reads:  make(chan []byte, 16),
		writes: make(chan testFrame, 512),
		closed: make(chan struct{}),
	}
}

func TestIsUnexpectedReadClose(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "normal closure", err: &websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "client exit"}},
		{name: "going away", err: &websocket.CloseError{Code: websocket.CloseGoingAway, Text: "shutdown"}},
		{name: "abnormal closure", err: &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"}, want: true},
		{name: "protocol error", err: &websocket.CloseError{Code: websocket.CloseProtocolError, Text: "invalid frame"}, want: true},
		{name: "read error", err: io.ErrUnexpectedEOF, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnexpectedReadClose(tt.err); got != tt.want {
				t.Fatalf("isUnexpectedReadClose() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (m *mockConn) ReadMessage() (int, []byte, error) {
	select {
	case <-m.closed:
		return 0, nil, io.EOF
	case message := <-m.reads:
		return websocket.TextMessage, bytes.Clone(message), nil
	}
}

func (m *mockConn) WriteMessage(messageType int, data []byte) error {
	if m.writeGate != nil {
		select {
		case <-m.writeGate:
		case <-m.closed:
			return io.ErrClosedPipe
		}
	}
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	case m.writes <- testFrame{messageType: messageType, data: bytes.Clone(data)}:
		return nil
	}
}

func (m *mockConn) SetReadLimit(int64) {}

func (m *mockConn) SetReadDeadline(time.Time) error { return m.readDeadlineErr }

func (m *mockConn) SetWriteDeadline(time.Time) error { return m.writeDeadlineErr }

func (m *mockConn) SetPongHandler(handler func(string) error) {
	m.mu.Lock()
	m.pongHandler = handler
	m.mu.Unlock()
}

func (m *mockConn) Close() error {
	m.closeOnce.Do(func() {
		m.closeCount.Add(1)
		close(m.closed)
	})
	return nil
}

type lifecycleCoordinator struct {
	decrements atomic.Int32
	executor   *pool.PartitionedExecutor
}

func (*lifecycleCoordinator) GetShardID(string) string { return "test-shard" }
func (*lifecycleCoordinator) GetPubSubManager() pubsub.Manager {
	return pubsub.NewNoopManager()
}
func (c *lifecycleCoordinator) GetEventExecutor() *pool.PartitionedExecutor {
	if c.executor == nil {
		c.executor = pool.NewPartitionedExecutor(2, 32)
	}
	return c.executor
}
func (c *lifecycleCoordinator) DecrTotalConnections() { c.decrements.Add(1) }

func newRunningTestConnection(t *testing.T, transport *mockConn) (*Connection, *Shard, *lifecycleCoordinator) {
	t.Helper()
	coordinator := &lifecycleCoordinator{executor: pool.NewPartitionedExecutor(2, 32)}
	t.Cleanup(func() { _ = coordinator.executor.Shutdown(context.Background()) })
	shard := NewShard("test-shard", coordinator)
	connection := NewConnection(transport, shard, "user-123", shard.name, "127.0.0.1", "node-1", "req-1")
	shard.activeConns[connection] = struct{}{}
	shard.userSessions[connection.userID] = map[*Connection]struct{}{connection: {}}
	atomic.StoreInt64(&shard.connectionCount, 1)
	go connection.Run()
	return connection, shard, coordinator
}

func waitConnection(t *testing.T, connection *Connection) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := connection.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionLifecycle(t *testing.T) {
	transport := newMockConn()
	connection, shard, coordinator := newRunningTestConnection(t, transport)

	if connection.GetUserID() != "user-123" || connection.GetShardID() != "test-shard" {
		t.Fatalf("unexpected connection metadata: user=%q shard=%q", connection.GetUserID(), connection.GetShardID())
	}

	connection.Close("test shutdown")
	waitConnection(t, connection)
	select {
	case <-connection.writerDone:
	default:
		t.Fatal("lifecycle completed before writer exited")
	}
	if !connection.IsClosed() || transport.closeCount.Load() != 1 {
		t.Fatalf("closed=%v transport closes=%d", connection.IsClosed(), transport.closeCount.Load())
	}
	if shard.GetConnectionCount() != 0 || coordinator.decrements.Load() != 1 {
		t.Fatalf("shard count=%d manager decrements=%d", shard.GetConnectionCount(), coordinator.decrements.Load())
	}
}

func TestConnectionWritesOneMessagePerFrameInOrder(t *testing.T) {
	transport := newMockConn()
	connection, _, _ := newRunningTestConnection(t, transport)

	for _, message := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		if !connection.Send(message) {
			t.Fatalf("failed to enqueue %q", message)
		}
	}
	for _, want := range []string{"one", "two", "three"} {
		select {
		case frame := <-transport.writes:
			if frame.messageType != websocket.TextMessage || string(frame.data) != want {
				t.Fatalf("frame = (%d, %q), want text %q", frame.messageType, frame.data, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for frame %q", want)
		}
	}
	connection.Close("test complete")
	waitConnection(t, connection)
}

func TestConnectionSendClonesPayload(t *testing.T) {
	transport := newMockConn()
	transport.writeGate = make(chan struct{})
	connection, _, _ := newRunningTestConnection(t, transport)
	payload := []byte("private-message")
	if !connection.Send(payload) {
		t.Fatal("failed to enqueue payload")
	}
	copy(payload, []byte("mutated-content"))
	close(transport.writeGate)
	select {
	case frame := <-transport.writes:
		if string(frame.data) != "private-message" {
			t.Fatalf("written payload = %q", frame.data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cloned payload")
	}
	connection.Close("test complete")
	waitConnection(t, connection)
}

func TestConnectionOutboundQueuesAreIsolated(t *testing.T) {
	firstTransport := newMockConn()
	firstTransport.writeGate = make(chan struct{})
	first, _, _ := newRunningTestConnection(t, firstTransport)
	if !first.Send([]byte("first-user-only")) {
		t.Fatal("failed to enqueue first connection payload")
	}
	first.Close("replace connection")

	secondTransport := newMockConn()
	second, _, _ := newRunningTestConnection(t, secondTransport)
	if !second.Send([]byte("second-user-only")) {
		t.Fatal("failed to enqueue second connection payload")
	}
	select {
	case frame := <-secondTransport.writes:
		if string(frame.data) != "second-user-only" {
			t.Fatalf("second connection received stale payload %q", frame.data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second connection payload")
	}
	select {
	case frame := <-secondTransport.writes:
		t.Fatalf("second connection received extra payload %q", frame.data)
	default:
	}

	close(firstTransport.writeGate)
	waitConnection(t, first)
	second.Close("test complete")
	waitConnection(t, second)
}

func TestConnectionConcurrentSendAndClose(t *testing.T) {
	transport := newMockConn()
	connection, _, coordinator := newRunningTestConnection(t, transport)

	var senders sync.WaitGroup
	for i := 0; i < 32; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for j := 0; j < 100; j++ {
				connection.Send([]byte("message"))
			}
		}()
	}
	for i := 0; i < 32; i++ {
		go connection.Close("concurrent close")
	}
	senders.Wait()
	waitConnection(t, connection)
	if transport.closeCount.Load() != 1 || coordinator.decrements.Load() != 1 {
		t.Fatalf("transport closes=%d manager decrements=%d", transport.closeCount.Load(), coordinator.decrements.Load())
	}
}

func TestConnectionQueueFullIsNonBlocking(t *testing.T) {
	transport := newMockConn()
	transport.writeGate = make(chan struct{})
	connection, _, _ := newRunningTestConnection(t, transport)

	for i := 0; i < defaultSendQueueSize+1; i++ {
		connection.Send([]byte("queued"))
	}
	start := time.Now()
	if connection.Send([]byte("overflow")) {
		t.Fatal("expected full queue to reject send")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("full queue send blocked")
	}
	if !connection.IsClosed() {
		t.Fatal("slow consumer was not disconnected")
	}
	close(transport.writeGate)
	waitConnection(t, connection)
}

func TestShardRemoveConnectionIsIdempotent(t *testing.T) {
	transport := newMockConn()
	connection, shard, coordinator := newRunningTestConnection(t, transport)
	connection.Close("test complete")
	waitConnection(t, connection)
	if shard.removeConnection(connection, "duplicate unregister") {
		t.Fatal("duplicate unregister removed the connection twice")
	}
	if coordinator.decrements.Load() != 1 || shard.GetConnectionCount() != 0 {
		t.Fatalf("manager decrements=%d shard count=%d", coordinator.decrements.Load(), shard.GetConnectionCount())
	}
}

func TestConnectionReadDeadlineFailureStopsLifecycle(t *testing.T) {
	transport := newMockConn()
	transport.readDeadlineErr = errors.New("deadline failed")
	connection, _, _ := newRunningTestConnection(t, transport)
	waitConnection(t, connection)
	if transport.closeCount.Load() != 1 {
		t.Fatalf("transport closes=%d", transport.closeCount.Load())
	}
}
