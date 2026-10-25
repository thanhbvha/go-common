package transport

import "time"

const MaxSchedulerErrorBytes = 512

// SchedulerStatus is a payload-free snapshot of one transport-owned scheduler
// worker. Counters are monotonic for the lifetime of the transport instance.
type SchedulerStatus struct {
	Transport          string
	Topology           string
	Worker             string
	LastPoll           time.Time
	LastSuccess        time.Time
	LastErrorAt        time.Time
	LastErrorOperation string
	LastError          string
	Polls              uint64
	Dispatches         uint64
	Transfers          uint64
	Commits            uint64
	Retries            uint64
	ConsecutiveErrors  uint64
	Degraded           bool
}

// SchedulerHealthReader exposes bounded operational state for autonomous delay
// schedulers. LastError is a bounded error type/class, not raw error text;
// implementations must not include payloads, credentials, or broker connection
// strings.
type SchedulerHealthReader interface {
	SchedulerHealth(topology Topology) []SchedulerStatus
}
