package wshttp

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

// blockingConn is a core.Conn whose reads block until Close, like an idle client.
type blockingConn struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingConn() *blockingConn { return &blockingConn{closed: make(chan struct{})} }

func (c *blockingConn) ReadMessage() (int, []byte, error) {
	<-c.closed
	return 0, nil, io.EOF
}
func (c *blockingConn) WriteMessage(int, []byte) error    { return nil }
func (c *blockingConn) SetReadLimit(int64)                {}
func (c *blockingConn) SetReadDeadline(time.Time) error   { return nil }
func (c *blockingConn) SetWriteDeadline(time.Time) error  { return nil }
func (c *blockingConn) SetPongHandler(func(string) error) {}
func (c *blockingConn) Close() error                      { c.once.Do(func() { close(c.closed) }); return nil }

func newManager(t *testing.T, config core.ManagerConfig) *core.Manager {
	t.Helper()
	manager := core.NewManagerWithConfig(config)
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

func TestGateAdmitsHealthyRequest(t *testing.T) {
	gate := NewGate(newManager(t, core.DefaultManagerConfig()), nil)
	if denial := gate.Admit("10.0.0.1"); denial != nil {
		t.Fatalf("healthy request denied: %+v", denial)
	}
}

func TestGateRateLimitsPerSourceBeforeAuthentication(t *testing.T) {
	gate := NewGate(newManager(t, core.DefaultManagerConfig()), limiter.NewKeyedRateLimiter(0.001, 2, 0, 0))
	gate.Admit("attacker")
	gate.Admit("attacker")
	denial := gate.Admit("attacker")
	if denial == nil || denial.Status != 429 || denial.RetryAfterHeader() != "1" {
		t.Fatalf("denial = %+v", denial)
	}
	if gate.Admit("honest") != nil {
		t.Fatal("honest source throttled by another source")
	}
}

func TestGateRejectsWithRetryAfterWhenDraining(t *testing.T) {
	manager := newManager(t, core.DefaultManagerConfig())
	gate := NewGate(manager, nil)
	go func() { _ = manager.Shutdown(context.Background()) }()
	deadline := time.Now().Add(2 * time.Second)
	var denial *Denial
	for time.Now().Before(deadline) {
		if denial = gate.Admit("ip"); denial != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if denial == nil || denial.Status != 503 || denial.RetryAfterHeader() != "5" {
		t.Fatalf("denial = %+v", denial)
	}
}

func TestGateRejectsWhenNodeAtCapacity(t *testing.T) {
	config := core.DefaultManagerConfig()
	config.MaxConnections = 1
	manager := newManager(t, config)
	gate := NewGate(manager, nil)
	if gate.Admit("ip") != nil {
		t.Fatal("empty node should admit")
	}
	conn := newBlockingConn()
	t.Cleanup(func() { _ = conn.Close() })
	go func() { _ = manager.HandleConnection(conn, "", "user", "127.0.0.1", "req") }()
	deadline := time.Now().Add(2 * time.Second)
	for manager.GetTotalConnections() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("connection was never registered")
		}
		time.Sleep(time.Millisecond)
	}
	denial := gate.Admit("ip")
	if denial == nil || denial.Status != 503 || denial.Message != "server at capacity" {
		t.Fatalf("denial = %+v", denial)
	}
}

func TestRetryAfterHeaderHasOneSecondFloor(t *testing.T) {
	if got := (Denial{RetryAfter: 10 * time.Millisecond}).RetryAfterHeader(); got != "1" {
		t.Fatalf("got %q", got)
	}
}
