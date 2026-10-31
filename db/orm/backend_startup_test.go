package orm

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/db/orm/backend"
	mysqlbackend "github.com/thanhbvha/go-common/db/orm/backend/mysql"
)

func TestMySQLStartupHandshakeHonorsOverallDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	cfg := DefaultConfig()
	cfg.StartupTimeout = 40 * time.Millisecond
	cfg.Backend = mysqlbackend.New("user@tcp(" + listener.Addr().String() + ")/app")
	started := time.Now()
	c, err := Open(context.Background(), cfg)
	if c != nil {
		_ = c.Close(context.Background())
		t.Fatal("blackhole handshake opened")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup deadline lost: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("startup was unbounded")
	}
	select {
	case conn := <-accepted:
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("handshake connection not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("handshake was not attempted")
	}
}
func TestRequirementsAndCapabilitiesAreExplicit(t *testing.T) {
	cfg := testConfig()
	cfg.Require = backend.Requirements{ReadOnlyTransactions: true}
	if err := cfg.Validate(); !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatal(err)
	}
	c := openTest(t)
	caps := c.Capabilities()
	caps.IsolationLevels[0] = 999
	if c.Capabilities().IsolationLevels[0] == 999 {
		t.Fatal("capabilities leaked mutable slice")
	}
}
