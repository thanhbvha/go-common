package nethttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

func secureServer(t *testing.T, config core.ManagerConfig, identity func() core.Identity) (string, *core.Manager) {
	t.Helper()
	manager := core.NewManagerWithConfig(config)
	handler := NewHandler(Config{
		Manager:              manager,
		AuthenticateIdentity: func(*http.Request) (core.Identity, error) { return identity(), nil },
		ConnectionLimiter:    limiter.NewConnectionLimiter(1000),
	})
	server := httptest.NewServer(http.HandlerFunc(handler.HandleUpgrade))
	t.Cleanup(func() { server.Close(); _ = manager.Shutdown(t.Context()) })
	return "ws" + strings.TrimPrefix(server.URL, "http"), manager
}

// A token that expires while its socket is open must be cut off with a policy
// close (1008), not left connected until the client disconnects.
func TestTokenExpiryClosesLiveConnectionWithPolicyViolation(t *testing.T) {
	expiry := time.Now().Add(400 * time.Millisecond)
	url, _ := secureServer(t, core.DefaultManagerConfig(), func() core.Identity {
		return core.Identity{TenantID: "t", UserID: "u", SessionID: "s", ExpiresAt: expiry}
	})
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok || closeErr.Code != websocket.ClosePolicyViolation {
		t.Fatalf("read error = %v, want close 1008", err)
	}
}

// An already-expired identity must never get a socket.
func TestExpiredIdentityIsRefusedAtUpgrade(t *testing.T) {
	url, manager := secureServer(t, core.DefaultManagerConfig(), func() core.Identity {
		return core.Identity{TenantID: "t", UserID: "u", SessionID: "s", ExpiresAt: time.Now().Add(-time.Second)}
	})
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _, err = conn.ReadMessage() // refused after upgrade is also acceptable, but it must not stay open
		_ = conn.Close()
		if err == nil {
			t.Fatal("expired identity kept a live socket")
		}
	}
	if n := manager.Snapshot().Connections; n != 0 {
		t.Fatalf("connections = %d, want 0", n)
	}
}

// Concurrent upgrades for one user can never exceed the per-user quota, however
// the requests interleave.
func TestPerUserQuotaHoldsUnderConcurrentUpgrades(t *testing.T) {
	const quota, attempts = 3, 40
	config := core.DefaultManagerConfig()
	config.MaxConnectionsPerUser = quota
	var next atomic.Int64
	url, manager := secureServer(t, config, func() core.Identity {
		// Distinct sessions of one user, so only the quota can stop them.
		return core.Identity{TenantID: "t", UserID: "same-user", SessionID: string(rune('a'+next.Add(1)%26)) + time.Now().Format("150405.000000000"), ExpiresAt: time.Now().Add(time.Hour)}
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var conns []*websocket.Conn
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err == nil {
				mu.Lock()
				conns = append(conns, c)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // sockets that lost the race are closed after upgrade
	open := 0
	for _, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if _, _, err := c.ReadMessage(); err != nil {
			if _, closed := err.(*websocket.CloseError); closed {
				continue
			}
		}
		open++
		defer c.Close()
	}
	if got := manager.Snapshot().Connections; got > quota {
		t.Fatalf("registered connections = %d, quota %d", got, quota)
	}
	if open > quota {
		t.Fatalf("%d sockets still open, quota %d", open, quota)
	}
}
