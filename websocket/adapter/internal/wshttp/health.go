package wshttp

import (
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
func ReadyStatus(readiness core.Readiness) (int, map[string]any) {
	status, label := http.StatusOK, "ready"
	if !readiness.Ready {
		status, label = http.StatusServiceUnavailable, "not_ready"
	}
	return status, map[string]any{"status": label, "checks": readiness}
}
