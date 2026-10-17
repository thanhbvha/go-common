package memory

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func init() {
	registry.Register("memory", func() (transport.Transport, error) {
		return New(), nil
	})
}

type memoryTransport struct {
	mu      sync.RWMutex
	queues  map[string]map[string]chan transport.Message // topologyName -> groupName -> chan
	dlqs    map[string][]transport.DeadLetter
	dedupes map[string]time.Time
	counts  map[string]int
	dlqSeq  uint64

	closeCtx context.Context
	cancel   context.CancelFunc
}

var (
	_ transport.Transport         = (*memoryTransport)(nil)
	_ transport.Deduper           = (*memoryTransport)(nil)
	_ transport.DLQAdmin          = (*memoryTransport)(nil)
	_ transport.StatsReader       = (*memoryTransport)(nil)
	_ transport.TopologyValidator = (*memoryTransport)(nil)
	_ transport.Delivery          = (*memoryDelivery)(nil)
)

// New returns a new in-memory transport for testing.
func New() transport.Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &memoryTransport{
		queues:   make(map[string]map[string]chan transport.Message),
		dlqs:     make(map[string][]transport.DeadLetter),
		dedupes:  make(map[string]time.Time),
		counts:   make(map[string]int),
		closeCtx: ctx,
		cancel:   cancel,
	}
}

func (m *memoryTransport) Name() string { return "memory" }

func (m *memoryTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		DelayPrecision: 1 * time.Millisecond,
		Ordering:       false,
	}
}

func (m *memoryTransport) ValidateTopology(topology transport.Topology) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	for key := range topology.Options {
		if strings.HasPrefix(key, "memory.") {
			return fmt.Errorf("memory: unknown option %q", key)
		}
	}
	return nil
}

func (m *memoryTransport) EnsureTopology(ctx context.Context, t transport.Topology) error {
	if err := m.ValidateTopology(t); err != nil {
		return err
	}
	name := t.Name()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[name]; !ok {
		m.queues[name] = make(map[string]chan transport.Message)
		if t.Kind == transport.TaskTopology {
			// Tasks always have one competing-consumer group. Event subscriber
			// groups are created by Consume and must not receive a phantom copy.
			m.queues[name][""] = make(chan transport.Message, 1000)
		}
	}
	return nil
}

func (m *memoryTransport) Publish(ctx context.Context, t transport.Topology, msg transport.Message) error {
	name := t.Name()

	m.mu.RLock()
	groups, ok := m.queues[name]
	m.mu.RUnlock()

	if !ok {
		m.EnsureTopology(ctx, t)
		m.mu.RLock()
		groups = m.queues[name]
		m.mu.RUnlock()
	}

	for _, q := range groups {
		select {
		case q <- msg:
		default:
			// Full, drop or error? Drop for now or skip.
		}
	}
	return nil
}

func (m *memoryTransport) PublishAt(ctx context.Context, t transport.Topology, msg transport.Message, runAt time.Time) error {
	delay := time.Until(runAt)
	if delay <= 0 {
		return m.Publish(ctx, t, msg)
	}

	go func() {
		select {
		case <-time.After(delay):
			_ = m.Publish(context.Background(), t, msg)
		case <-m.closeCtx.Done():
		}
	}()
	return nil
}

func (m *memoryTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	name := sub.Topology.Name()
	group := sub.Group

	m.mu.Lock()
	if _, ok := m.queues[name]; !ok {
		m.queues[name] = make(map[string]chan transport.Message)
	}
	if _, ok := m.queues[name][group]; !ok {
		m.queues[name][group] = make(chan transport.Message, 1000)
	}
	q := m.queues[name][group]
	m.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.closeCtx.Done():
			return m.closeCtx.Err()
		case msg := <-q:
			m.mu.Lock()
			count := m.counts[msg.ID] + 1
			m.counts[msg.ID] = count
			m.mu.Unlock()

			deliver(&memoryDelivery{
				transport: m,
				msg:       msg,
				topo:      sub.Topology,
				count:     count,
			})
		}
	}
}

func (m *memoryTransport) Reserve(ctx context.Context, dedupeKey string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exp, exists := m.dedupes[dedupeKey]
	if exists && time.Now().Before(exp) {
		return false, nil // already acquired
	}

	m.dedupes[dedupeKey] = time.Now().Add(ttl)
	return true, nil
}

func (m *memoryTransport) Release(ctx context.Context, dedupeKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.dedupes, dedupeKey)
	return nil
}

func (m *memoryTransport) List(ctx context.Context, topology transport.Topology, offset, limit int) ([]transport.DeadLetter, error) {
	name := topology.Name()
	m.mu.RLock()
	defer m.mu.RUnlock()
	dlq := m.dlqs[name]
	if offset > len(dlq) {
		return nil, nil
	}
	end := offset + limit
	if end > len(dlq) {
		end = len(dlq)
	}
	return dlq[offset:end], nil
}

func (m *memoryTransport) Requeue(ctx context.Context, topology transport.Topology, messageIDs []string) error {
	selected := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		selected[id] = struct{}{}
	}

	name := topology.Name()
	m.mu.Lock()
	records := m.dlqs[name]
	remaining := make([]transport.DeadLetter, 0, len(records))
	requeue := make([]transport.Message, 0, len(messageIDs))
	for _, record := range records {
		_, selectedByAdminID := selected[record.ID]
		_, selectedByLegacyMessageID := selected[record.Message.ID]
		if selectedByAdminID || selectedByLegacyMessageID {
			requeue = append(requeue, record.Message)
			continue
		}
		remaining = append(remaining, record)
	}
	m.dlqs[name] = remaining
	m.mu.Unlock()

	for _, message := range requeue {
		if err := m.Publish(ctx, topology, message); err != nil {
			return err
		}
	}
	return nil
}

func (m *memoryTransport) Purge(ctx context.Context, topology transport.Topology) error {
	name := topology.Name()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqs[name] = nil
	return nil
}

func (m *memoryTransport) Count(ctx context.Context, topology transport.Topology) (int64, error) {
	name := topology.Name()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.dlqs[name])), nil
}

func (m *memoryTransport) Stats(ctx context.Context, t transport.Topology) (transport.TopologyStats, error) {
	name := t.Name()
	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := m.queues[name]

	var pending int64
	for _, q := range groups {
		pending += int64(len(q))
	}

	dlq := m.dlqs[name]
	return transport.TopologyStats{
		Pending: pending,
		DLQ:     int64(len(dlq)),
	}, nil
}

func (m *memoryTransport) Ping(ctx context.Context) error {
	return nil
}

func (m *memoryTransport) Close(ctx context.Context) error {
	m.cancel()
	return nil
}

// memoryDelivery implements Delivery
type memoryDelivery struct {
	transport *memoryTransport
	msg       transport.Message
	topo      transport.Topology
	count     int
	settled   transport.SettlementGuard
}

func (d *memoryDelivery) Message() transport.Message { return d.msg }
func (d *memoryDelivery) DeliveryAttempt() int       { return d.count }
func (d *memoryDelivery) Heartbeat(context.Context) error {
	return nil
}

func (d *memoryDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		switch decision.Kind {
		case transport.DecisionAck:
			return nil
		case transport.DecisionRetry:
			return d.transport.PublishAt(ctx, d.topo, decision.Next, decision.RetryAt)
		case transport.DecisionDeadLetter:
			msg := d.msg
			if decision.Source != nil {
				msg = *decision.Source
			}
			lastError := ""
			if decision.Cause != nil {
				lastError = decision.Cause.Error()
			}
			failedAt := decision.FailedAt
			if failedAt.IsZero() {
				failedAt = time.Now()
			}
			name := d.topo.Name()
			d.transport.mu.Lock()
			d.transport.dlqSeq++
			record := transport.DeadLetter{
				ID:        strconv.FormatUint(d.transport.dlqSeq, 10),
				Message:   msg,
				Reason:    decision.Reason,
				LastError: lastError,
				Attempts:  decision.Attempts,
				FailedAt:  failedAt,
			}
			d.transport.dlqs[name] = append(d.transport.dlqs[name], record)
			d.transport.mu.Unlock()
			return nil
		default:
			return transport.ErrInvalidDecision
		}
	})
}
