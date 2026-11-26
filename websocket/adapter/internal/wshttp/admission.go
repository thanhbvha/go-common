package wshttp

import (
	"strconv"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

// Default per-source upgrade budget applied before authentication. It is
// deliberately generous for one NAT'd office or mobile carrier and exists to
// stop a single source from burning authentication CPU or I/O.
const (
	DefaultIPRate  = 100.0
	DefaultIPBurst = 200
)

// Denial describes an upgrade rejected before authentication.
type Denial struct {
	Status     int
	Message    string
	RetryAfter time.Duration
}

// RetryAfterHeader renders the Retry-After value in whole seconds (minimum 1).
func (d Denial) RetryAfterHeader() string {
	seconds := int(d.RetryAfter / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}

// Gate runs the checks that must happen before an upgrade request is
// authenticated: lifecycle, node capacity, and a per-source rate limit. Doing
// them first means unauthenticated or overload traffic never reaches the
// authenticator, and rejected clients get an HTTP status instead of a
// WebSocket that opens and is immediately closed.
type Gate struct {
	Manager *core.Manager
	IPRate  *limiter.KeyedRateLimiter
}

// NewGate builds a Gate. A nil ipRate selects the default per-source budget.
func NewGate(manager *core.Manager, ipRate *limiter.KeyedRateLimiter) Gate {
	if ipRate == nil {
		ipRate = limiter.NewKeyedRateLimiter(DefaultIPRate, DefaultIPBurst, 0, 0)
	}
	return Gate{Manager: manager, IPRate: ipRate}
}

// Admit returns nil when the request may proceed to authentication. key must
// identify the network source and must be derivable without authenticating.
func (g Gate) Admit(key string) *Denial {
	if !g.Manager.IsAcceptingConnections() {
		return &Denial{Status: 503, Message: "service draining", RetryAfter: 5 * time.Second}
	}
	if !g.Manager.CanAcceptConnection() {
		return &Denial{Status: 503, Message: "server at capacity", RetryAfter: 5 * time.Second}
	}
	if !g.IPRate.Allow(key) {
		return &Denial{Status: 429, Message: "rate limit exceeded", RetryAfter: time.Second}
	}
	return nil
}

// AdmitIdentity runs the post-authentication pre-check (per-user/account/tenant
// quota, shard capacity) so the rejection is an HTTP 503 instead of a socket
// that opens and is closed without a reason. It is advisory; the manager still
// enforces the limits atomically after the upgrade.
func (g Gate) AdmitIdentity(identity core.Identity) *Denial {
	err := g.Manager.CheckAdmission(identity, g.Manager.GetShardID(identity.UserID))
	if err == nil {
		return nil
	}
	return &Denial{Status: 503, Message: err.Error(), RetryAfter: 5 * time.Second}
}
