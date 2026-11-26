package wshttp

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"

	"github.com/thanhbvha/go-common/v2/websocket/core"
)

// LiveStatus answers a liveness probe. Liveness means "this process can serve an
// HTTP request", so it deliberately consults nothing else: not the manager state,
// and above all not a broker. If it failed whenever Redis or NATS was down, an
// orchestrator would restart every healthy node during a broker outage, turning a
// degraded cluster into a full one. A node that is draining must also stay live,
// or it would be killed before its connections finish draining.
func LiveStatus() (int, map[string]string) {
	return http.StatusOK, map[string]string{"status": "alive"}
}

// ReadyStatus answers a readiness probe: whether this node should receive new
// connections. It fails while the manager is not running, its executor or broker
// is unavailable, or the connection counters disagree beyond the grace period.
// The body carries only the verdict: which dependency failed is topology
// information, available to operators through the protected stats endpoint.
func ReadyStatus(readiness core.Readiness) (int, map[string]string) {
	status, label := http.StatusOK, "ready"
	if !readiness.Ready {
		status, label = http.StatusServiceUnavailable, "not_ready"
	}
	return status, map[string]string{"status": label}
}

// AdminAllowed reports whether header is "Bearer <token>". An empty token never
// matches, so operational endpoints stay closed until the application sets one.
func AdminAllowed(header, token string) bool {
	if token == "" {
		return false
	}
	got, want := sha256.Sum256([]byte(header)), sha256.Sum256([]byte("Bearer "+token))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}
