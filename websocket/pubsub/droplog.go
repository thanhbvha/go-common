package pubsub

import (
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/logger"
)

// DropLog reports rejected dispatches at most once per interval, so a full
// executor under load does not turn into one log line per message.
type DropLog struct {
	last       atomic.Int64
	suppressed atomic.Uint64
}

const dropLogInterval = time.Second

// Report logs err, or counts it silently when one was logged recently.
func (d *DropLog) Report(err error) {
	now := time.Now().UnixNano()
	last := d.last.Load()
	if now-last < int64(dropLogInterval) || !d.last.CompareAndSwap(last, now) {
		d.suppressed.Add(1)
		return
	}
	logger.ErrorAsync("WebSocket pubsub dispatch rejected", "error", err, "suppressed", d.suppressed.Swap(0))
}
