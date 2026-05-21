// package core contains the core WebSocket sharding, connection lifecycle, and manager logic.
package core

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// mockWriteCloser implements io.WriteCloser for mock connection testing.
type mockWriteCloser struct {
	buf *bytes.Buffer
}

func (m *mockWriteCloser) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockWriteCloser) Close() error {
	return nil
}

// mockConn satisfies the core.Conn interface for testing.
type mockConn struct {
	readChan  chan []byte
	writeChan chan []byte
	closed    bool
}

func newMockConn() *mockConn {
	return &mockConn{
		readChan:  make(chan []byte, 10),
		writeChan: make(chan []byte, 10),
	}
}

func (m *mockConn) ReadMessage() (messageType int, p []byte, err error) {
	msg, ok := <-m.readChan
	if !ok {
		return 0, nil, io.EOF
	}
	return 1, msg, nil // TextMessage
}

func (m *mockConn) WriteMessage(messageType int, data []byte) error {
	m.writeChan <- data
	return nil
}

func (m *mockConn) NextWriter(messageType int) (io.WriteCloser, error) {
	return &mockWriteCloser{buf: new(bytes.Buffer)}, nil
}

func (m *mockConn) SetReadLimit(limit int64) {}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetPongHandler(h func(string) error) {}

func (m *mockConn) Close() error {
	m.closed = true
	close(m.readChan)
	return nil
}

func TestConnectionLifecycle(t *testing.T) {
	manager := GetGlobalManager()
	shard := manager.GetOrCreateShard("test-shard")

	conn := newMockConn()
	connection := NewConnection(conn, shard, "user-123", "test-shard", "127.0.0.1", "node-1", "req-1")

	if connection == nil {
		t.Fatal("Expected connection wrapper to be allocated, got nil")
	}

	if connection.GetUserID() != "user-123" {
		t.Errorf("Expected userID user-123, got %s", connection.GetUserID())
	}

	if connection.GetShardID() != "test-shard" {
		t.Errorf("Expected shardID test-shard, got %s", connection.GetShardID())
	}

	// Register with Shard
	shard.Register(connection)
	time.Sleep(10 * time.Millisecond) // Let goroutine register it

	if shard.GetConnectionCount() != 1 {
		t.Errorf("Expected connection count 1, got %d", shard.GetConnectionCount())
	}

	// Close Connection
	connection.Close("graceful testing shutdown")
	time.Sleep(10 * time.Millisecond)

	if !connection.IsClosed() {
		t.Error("Expected connection.closed to be true")
	}
}
