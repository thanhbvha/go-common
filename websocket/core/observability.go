package core

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/websocket/pubsub"
)

// RuntimeMetrics contains low-cardinality cumulative WebSocket counters.
type RuntimeMetrics struct {
	ConnectionsOpened uint64 `json:"connectionsOpened"`
	ConnectionsClosed uint64 `json:"connectionsClosed"`
	InboundMessages   uint64 `json:"inboundMessages"`
	InboundBytes      uint64 `json:"inboundBytes"`
	OutboundMessages  uint64 `json:"outboundMessages"`
	OutboundBytes     uint64 `json:"outboundBytes"`
	OutboundDropped   uint64 `json:"outboundDropped"`
	SlowConsumers     uint64 `json:"slowConsumers"`
}

// Observer receives low-cardinality runtime and event outcomes.
type Observer interface {
	ConnectionOpened()
	ConnectionClosed()
	InboundMessage(int)
	OutboundMessage(int)
	OutboundDropped(bool)
	EventMalformed()
	EventUnknown()
	EventSubmitted()
	EventDropped()
	EventError()
	EventTimeout()
	EventPanic()
}

type SnapshotObserver interface {
	Observer
	RuntimeMetrics() RuntimeMetrics
	EventStats() EventStats
}

type LatencyObserver interface{ ObserveEventLatency(time.Duration) }
type TraceObserver interface {
	StartEvent(context.Context, string) (context.Context, func(error))
}

type LatencyHistogram struct {
	Under1ms   uint64 `json:"under1ms"`
	Under5ms   uint64 `json:"under5ms"`
	Under10ms  uint64 `json:"under10ms"`
	Under50ms  uint64 `json:"under50ms"`
	Under100ms uint64 `json:"under100ms"`
	Over100ms  uint64 `json:"over100ms"`
}

type AtomicObserver struct {
	connectionsOpened atomic.Uint64
	connectionsClosed atomic.Uint64
	inboundMessages   atomic.Uint64
	inboundBytes      atomic.Uint64
	outboundMessages  atomic.Uint64
	outboundBytes     atomic.Uint64
	outboundDropped   atomic.Uint64
	slowConsumers     atomic.Uint64
	eventMalformed    atomic.Uint64
	eventUnknown      atomic.Uint64
	eventSubmitted    atomic.Uint64
	eventDropped      atomic.Uint64
	eventErrors       atomic.Uint64
	eventTimeouts     atomic.Uint64
	eventPanics       atomic.Uint64
	latencyBuckets    [6]atomic.Uint64
}

func (o *AtomicObserver) ConnectionOpened() { o.connectionsOpened.Add(1) }
func (o *AtomicObserver) ConnectionClosed() { o.connectionsClosed.Add(1) }
func (o *AtomicObserver) InboundMessage(bytes int) {
	o.inboundMessages.Add(1)
	o.inboundBytes.Add(uint64(bytes))
}
func (o *AtomicObserver) OutboundMessage(bytes int) {
	o.outboundMessages.Add(1)
	o.outboundBytes.Add(uint64(bytes))
}
func (o *AtomicObserver) OutboundDropped(slow bool) {
	o.outboundDropped.Add(1)
	if slow {
		o.slowConsumers.Add(1)
	}
}
func (o *AtomicObserver) EventMalformed() { o.eventMalformed.Add(1) }
func (o *AtomicObserver) EventUnknown()   { o.eventUnknown.Add(1) }
func (o *AtomicObserver) EventSubmitted() { o.eventSubmitted.Add(1) }
func (o *AtomicObserver) EventDropped()   { o.eventDropped.Add(1) }
func (o *AtomicObserver) EventError()     { o.eventErrors.Add(1) }
func (o *AtomicObserver) EventTimeout()   { o.eventTimeouts.Add(1) }
func (o *AtomicObserver) EventPanic()     { o.eventPanics.Add(1) }
func (o *AtomicObserver) ObserveEventLatency(duration time.Duration) {
	index := 5
	for i, threshold := range [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		if duration < threshold {
			index = i
			break
		}
	}
	o.latencyBuckets[index].Add(1)
}
func (o *AtomicObserver) LatencyHistogram() LatencyHistogram {
	return LatencyHistogram{Under1ms: o.latencyBuckets[0].Load(), Under5ms: o.latencyBuckets[1].Load(), Under10ms: o.latencyBuckets[2].Load(), Under50ms: o.latencyBuckets[3].Load(), Under100ms: o.latencyBuckets[4].Load(), Over100ms: o.latencyBuckets[5].Load()}
}

func (o *AtomicObserver) RuntimeMetrics() RuntimeMetrics {
	return RuntimeMetrics{
		ConnectionsOpened: o.connectionsOpened.Load(), ConnectionsClosed: o.connectionsClosed.Load(),
		InboundMessages: o.inboundMessages.Load(), InboundBytes: o.inboundBytes.Load(),
		OutboundMessages: o.outboundMessages.Load(), OutboundBytes: o.outboundBytes.Load(),
		OutboundDropped: o.outboundDropped.Load(), SlowConsumers: o.slowConsumers.Load(),
	}
}

func (o *AtomicObserver) EventStats() EventStats {
	return EventStats{Malformed: o.eventMalformed.Load(), Unknown: o.eventUnknown.Load(), Submitted: o.eventSubmitted.Load(), Dropped: o.eventDropped.Load(), Errors: o.eventErrors.Load(), Timeouts: o.eventTimeouts.Load(), Panics: o.eventPanics.Load()}
}

var defaultObserver = &AtomicObserver{}

func GetRuntimeMetrics() RuntimeMetrics {
	return defaultObserver.RuntimeMetrics()
}

// Readiness is a low-cardinality operational readiness result.
type Readiness struct {
	Ready             bool         `json:"ready"`
	State             ManagerState `json:"state"`
	ExecutorAccepting bool         `json:"executorAccepting"`
	BrokerReady       bool         `json:"brokerReady"`
	AccountingValid   bool         `json:"accountingValid"`
}

func (m *Manager) Readiness() Readiness {
	stats := m.GetStats()
	executorReady := m.eventExecutor.Stats().Accepting
	brokerReady := true
	if provider, ok := m.pubsubManager.(pubsub.ReadinessProvider); ok {
		brokerReady = provider.Ready()
	}
	accountingValid := stats["totalConnections"] == stats["shardConnections"]
	state := m.State()
	return Readiness{Ready: state == StateRunning && executorReady && brokerReady && accountingValid, State: state, ExecutorAccepting: executorReady, BrokerReady: brokerReady, AccountingValid: accountingValid}
}
