package mongodb

import "time"

// TransactionEvent contains no command payload, URI or raw driver error.
// Phase is started, attempt or finished; Outcome is committed, failed or unknown.
type TransactionEvent struct {
	Phase       string
	ClientGroup string
	Attempt     int
	Duration    time.Duration
	Outcome     string
}

// TransactionMonitor callbacks run synchronously and must be fast, concurrency-safe
// and must not call manager lifecycle methods. Panics are isolated from DB work.
type TransactionMonitor struct{ Event func(TransactionEvent) }

type TransactionStats struct {
	Started          uint64
	Committed        uint64
	Failed           uint64
	Unknown          uint64
	CallbackAttempts uint64
	Retries          uint64
	Active           uint64
}

func (m *Manager) TransactionStats() TransactionStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats
}
func GetTransactionStats() TransactionStats { return globalManager.TransactionStats() }

func observe(monitor *TransactionMonitor, event TransactionEvent) {
	if monitor == nil || monitor.Event == nil {
		return
	}
	defer func() { _ = recover() }()
	monitor.Event(event)
}
