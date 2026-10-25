package memory

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	defaultMaxReadyEntries   = 1000
	defaultMaxDelayedEntries = 1000
	defaultMaxDLQEntries     = 1000
	memoryDelayDispatchBatch = 128
)

var errMemoryClosed = errors.New("memory: transport is closed")

func init() {
	registry.Register("memory", func() (transport.Transport, error) {
		return New(), nil
	})
}

type memoryTransport struct {
	mu          sync.Mutex
	topologies  map[string]*memoryTopology
	dlqs        map[string][]transport.DeadLetter
	dedupes     map[string]memoryDedupe
	dlqSeq      uint64
	deliverySeq uint64
	delaySeq    uint64

	delayed         memoryDelayHeap
	delayedCounts   map[string]int
	schedulerStatus MemorySchedulerStatus
	wake            chan struct{}
	closeCtx        context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	closed          bool
}

type memoryTopology struct {
	topology      transport.Topology
	groups        map[string]*memoryGroup
	orderedGroups []string
}

type memoryGroup struct {
	ready    []memoryQueued
	inFlight map[uint64]memoryInFlight
	notify   chan struct{}
}

type memoryQueued struct {
	message  transport.Message
	attempts int
	readyAt  time.Time
}

type memoryInFlight struct {
	queued  memoryQueued
	session uint64
}

type memoryDedupe struct {
	expires time.Time
	owner   string
	state   transport.DedupeReservationState
}

type memoryDelayed struct {
	topology transport.Topology
	message  transport.Message
	groups   []string
	runAt    time.Time
	seq      uint64
}

type memoryDelayHeap []memoryDelayed

// MemorySchedulerStatus is a payload-free snapshot of the process-local delay
// scheduler. A nonempty LastError means due work remains retained for retry.
type MemorySchedulerStatus struct {
	LastRun     time.Time
	LastSuccess time.Time
	LastError   string
	Retries     uint64
	Pending     int
}

func (h memoryDelayHeap) Len() int { return len(h) }
func (h memoryDelayHeap) Less(i, j int) bool {
	if h[i].runAt.Equal(h[j].runAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].runAt.Before(h[j].runAt)
}
func (h memoryDelayHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *memoryDelayHeap) Push(value any) { *h = append(*h, value.(memoryDelayed)) }
func (h *memoryDelayHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = memoryDelayed{}
	*h = old[:last]
	return value
}

var (
	_ transport.Transport               = (*memoryTransport)(nil)
	_ transport.Deduper                 = (*memoryTransport)(nil)
	_ transport.TopologyDeduper         = (*memoryTransport)(nil)
	_ transport.PublicationDeduper      = (*memoryTransport)(nil)
	_ transport.DLQAdmin                = (*memoryTransport)(nil)
	_ transport.StatsReader             = (*memoryTransport)(nil)
	_ transport.SubscriptionStatsReader = (*memoryTransport)(nil)
	_ transport.TopologyValidator       = (*memoryTransport)(nil)
	_ transport.SubscriptionProvisioner = (*memoryTransport)(nil)
	_ transport.Delivery                = (*memoryDelivery)(nil)
)

// New returns a bounded, process-local transport for tests and development.
func New() transport.Transport {
	ctx, cancel := context.WithCancel(context.Background())
	m := &memoryTransport{
		topologies:    make(map[string]*memoryTopology),
		dlqs:          make(map[string][]transport.DeadLetter),
		dedupes:       make(map[string]memoryDedupe),
		delayedCounts: make(map[string]int),
		wake:          make(chan struct{}, 1),
		closeCtx:      ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}
	heap.Init(&m.delayed)
	go m.scheduler()
	return m
}

func (m *memoryTransport) Name() string { return "memory" }

func (m *memoryTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		Durability:     transport.DurabilityProcess,
		Delay:          transport.DelayCapabilities{Supported: true, Precision: time.Millisecond, Provisioning: transport.DelaySharedScheduler, Optional: true},
		Settlement:     transport.SettlementCapabilities{Retry: transport.SettlementProcessLocal, DLQ: transport.SettlementProcessLocal},
		Dedupe:         transport.DedupeProcessConfirmed,
		Admin:          transport.AdminCapabilities{DLQ: true, CursorDLQ: true, Stats: true, SubscriptionStats: true, Pause: transport.ScopeProcess},
		Scheduler:      transport.SchedulerCapabilities{Health: true, WorkerScope: transport.ScopeProcess},
		Payload:        transport.PayloadCapabilities{CoreLimitSupported: true},
		DelayPrecision: time.Millisecond,
	}
}

func (m *memoryTransport) ValidateTopology(topology transport.Topology) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	for key, value := range topology.Options {
		switch key {
		case "memory.max_ready_entries", "memory.max_delayed_entries", "memory.max_dlq_entries":
			limit, ok := value.(int)
			if !ok || limit <= 0 {
				return fmt.Errorf("memory: %s must be a positive int", key)
			}
		default:
			if strings.HasPrefix(key, "memory.") {
				return fmt.Errorf("memory: unknown option %q", key)
			}
		}
	}
	return nil
}

func (m *memoryTransport) EnsureTopology(ctx context.Context, topology transport.Topology) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.ValidateTopology(topology); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	m.ensureTopologyLocked(topology)
	return nil
}

func (m *memoryTransport) EnsureSubscription(ctx context.Context, sub transport.Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.ValidateTopology(sub.Topology); err != nil {
		return err
	}
	if sub.Topology.Kind == transport.EventTopology && strings.TrimSpace(sub.Group) == "" {
		return fmt.Errorf("memory: event subscription group must not be blank")
	}
	if sub.Topology.Kind == transport.TaskTopology && sub.Group != "" {
		return fmt.Errorf("memory: task subscription group must be empty")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	topo := m.ensureTopologyLocked(sub.Topology)
	if _, ok := topo.groups[sub.Group]; !ok {
		topo.groups[sub.Group] = newMemoryGroup()
		topo.orderedGroups = append(topo.orderedGroups, sub.Group)
		slices.Sort(topo.orderedGroups)
	}
	return nil
}

func (m *memoryTransport) ensureTopologyLocked(topology transport.Topology) *memoryTopology {
	name := topology.Name()
	if existing := m.topologies[name]; existing != nil {
		return existing
	}
	created := &memoryTopology{topology: topology, groups: make(map[string]*memoryGroup)}
	if topology.Kind == transport.TaskTopology {
		created.groups[""] = newMemoryGroup()
		created.orderedGroups = []string{""}
	}
	m.topologies[name] = created
	return created
}

func newMemoryGroup() *memoryGroup {
	return &memoryGroup{inFlight: make(map[uint64]memoryInFlight), notify: make(chan struct{}, 1)}
}

func (m *memoryTransport) Publish(ctx context.Context, topology transport.Topology, msg transport.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.ValidateTopology(topology); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	topo := m.ensureTopologyLocked(topology)
	groups := topo.orderedGroups
	if topology.Kind == transport.EventTopology && len(groups) == 0 {
		return fmt.Errorf("%w: memory event has no process-local subscriber groups", transport.ErrNotSupported)
	}
	if err := m.enqueueLocked(topo, groups, cloneMessage(msg), time.Now(), "ready", nil); err != nil {
		return err
	}
	return nil
}

func (m *memoryTransport) PublishAt(ctx context.Context, topology transport.Topology, msg transport.Message, runAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.ValidateTopology(topology); err != nil {
		return err
	}
	if !runAt.After(time.Now()) {
		return m.Publish(ctx, topology, msg)
	}
	if disabled, _ := topology.Options["eventbus.delay_disabled"].(bool); disabled {
		return transport.ErrNotSupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	topo := m.ensureTopologyLocked(topology)
	groups := topo.orderedGroups
	if topology.Kind == transport.EventTopology && len(groups) == 0 {
		return fmt.Errorf("%w: memory event has no process-local subscriber groups", transport.ErrNotSupported)
	}
	return m.scheduleLocked(topology, groups, cloneMessage(msg), runAt, nil)
}

func (m *memoryTransport) scheduleLocked(topology transport.Topology, groups []string, msg transport.Message, runAt time.Time, source *memoryDelivery) error {
	name := topology.Name()
	current := m.delayedCounts[name]
	limit := memoryOption(topology, "memory.max_delayed_entries", defaultMaxDelayedEntries)
	if current >= limit {
		return memoryCapacityError(topology, "delayed", current, limit)
	}
	m.delaySeq++
	heap.Push(&m.delayed, memoryDelayed{topology: topology, message: msg, groups: append([]string(nil), groups...), runAt: runAt, seq: m.delaySeq})
	m.delayedCounts[name] = current + 1
	if source != nil {
		m.ackSourceLocked(source)
	}
	m.signalSchedulerLocked()
	return nil
}

func (m *memoryTransport) scheduler() {
	defer close(m.done)
	var timer *time.Timer
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			return
		}
		var wait time.Duration
		if len(m.delayed) == 0 {
			wait = time.Hour
		} else {
			wait = max(0, time.Until(m.delayed[0].runAt))
		}
		m.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(wait)
		}
		select {
		case <-m.closeCtx.Done():
			return
		case <-m.wake:
		case <-timer.C:
			if _, err := m.dispatchDue(context.Background(), time.Now()); err != nil {
				m.recordSchedulerError(err)
				timer.Reset(10 * time.Millisecond)
				select {
				case <-m.closeCtx.Done():
					return
				case <-m.wake:
				case <-timer.C:
				}
			}
		}
	}
}

func (m *memoryTransport) dispatchDue(ctx context.Context, now time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, errMemoryClosed
	}
	m.schedulerStatus.LastRun = now
	var moved int64
	for len(m.delayed) > 0 && !m.delayed[0].runAt.After(now) && moved < memoryDelayDispatchBatch {
		item := m.delayed[0]
		topo := m.ensureTopologyLocked(item.topology)
		if err := m.enqueueLocked(topo, item.groups, item.message, item.runAt, "ready", nil); err != nil {
			return moved, err
		}
		heap.Pop(&m.delayed)
		m.delayedCounts[item.topology.Name()]--
		moved++
	}
	if len(m.delayed) > 0 && !m.delayed[0].runAt.After(now) {
		m.signalSchedulerLocked()
	}
	m.schedulerStatus.LastSuccess = now
	m.schedulerStatus.LastError = ""
	m.schedulerStatus.Pending = len(m.delayed)
	return moved, nil
}

func (m *memoryTransport) recordSchedulerError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.schedulerStatus.LastError = err.Error()
	m.schedulerStatus.Retries++
	m.schedulerStatus.Pending = len(m.delayed)
}

// SchedulerStatus reports process-local delay health without message contents.
func (m *memoryTransport) SchedulerStatus() MemorySchedulerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.schedulerStatus
	status.Pending = len(m.delayed)
	return status
}

func (m *memoryTransport) enqueueLocked(topo *memoryTopology, groups []string, msg transport.Message, readyAt time.Time, resource string, source *memoryDelivery) error {
	limit := memoryOption(topo.topology, "memory.max_ready_entries", defaultMaxReadyEntries)
	for _, name := range groups {
		group := topo.groups[name]
		if group == nil {
			return fmt.Errorf("memory: subscription group %q is not provisioned", name)
		}
		current := len(group.ready) + len(group.inFlight)
		if source != nil && source.topologyName == topo.topology.Name() && source.group == name {
			current--
		}
		if current >= limit {
			return memoryCapacityError(topo.topology, resource, current, limit)
		}
	}
	for _, name := range groups {
		group := topo.groups[name]
		group.ready = append(group.ready, memoryQueued{message: msg, readyAt: readyAt})
		signal(group.notify)
	}
	if source != nil {
		m.ackSourceLocked(source)
	}
	return nil
}

func (m *memoryTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	if deliver == nil {
		return fmt.Errorf("memory: delivery callback is nil")
	}
	if err := m.EnsureSubscription(ctx, sub); err != nil {
		return err
	}
	session := m.nextSession()
	defer func() {
		if sub.WaitForHandlers != nil {
			sub.WaitForHandlers()
		}
		m.requeueSession(sub.Topology.Name(), sub.Group, session)
	}()
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return errMemoryClosed
		}
		topo := m.topologies[sub.Topology.Name()]
		group := topo.groups[sub.Group]
		if len(group.ready) > 0 {
			queued := group.ready[0]
			group.ready[0] = memoryQueued{}
			if len(group.ready) == 1 {
				group.ready = group.ready[:0]
			} else {
				group.ready = group.ready[1:]
			}
			queued.attempts++
			m.deliverySeq++
			id := m.deliverySeq
			group.inFlight[id] = memoryInFlight{queued: queued, session: session}
			delivery := &memoryDelivery{
				transport: m, msg: queued.message, topo: sub.Topology,
				topologyName: sub.Topology.Name(), group: sub.Group, id: id, count: queued.attempts,
			}
			m.mu.Unlock()
			deliver(delivery)
			continue
		}
		notify := group.notify
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.closeCtx.Done():
			return errMemoryClosed
		case <-notify:
		}
	}
}

func (m *memoryTransport) nextSession() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliverySeq++
	return m.deliverySeq
}

func (m *memoryTransport) requeueSession(topologyName, groupName string, session uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	topo := m.topologies[topologyName]
	if topo == nil || topo.groups[groupName] == nil {
		return
	}
	group := topo.groups[groupName]
	var restored []memoryQueued
	for id, inFlight := range group.inFlight {
		if inFlight.session == session {
			restored = append(restored, inFlight.queued)
			delete(group.inFlight, id)
		}
	}
	if len(restored) > 0 {
		group.ready = append(restored, group.ready...)
		signal(group.notify)
	}
}

func (m *memoryTransport) Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return m.reserve(ctx, key, ttl)
}

func (m *memoryTransport) Release(ctx context.Context, key string) error {
	return m.release(ctx, key)
}

func (m *memoryTransport) ReserveTopology(ctx context.Context, topo transport.Topology, key string, ttl time.Duration) (bool, error) {
	return m.reserve(ctx, memoryDedupeKey(topo, key), ttl)
}

func (m *memoryTransport) ReleaseTopology(ctx context.Context, topo transport.Topology, key string) error {
	return m.release(ctx, memoryDedupeKey(topo, key))
}

func (m *memoryTransport) reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if key == "" || ttl <= 0 {
		return false, fmt.Errorf("memory: dedupe key and positive TTL required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, errMemoryClosed
	}
	now := time.Now()
	if record, exists := m.dedupes[key]; exists && now.Before(record.expires) {
		return false, nil
	}
	m.dedupes[key] = memoryDedupe{expires: now.Add(ttl), state: transport.DedupeReservationPending}
	return true, nil
}

func (m *memoryTransport) release(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	delete(m.dedupes, key)
	return nil
}

func memoryDedupeKey(topo transport.Topology, key string) string {
	return topo.Name() + "\x00" + key
}

func (m *memoryTransport) BeginPublication(ctx context.Context, topo transport.Topology, key, reservationID string, ttl time.Duration) (transport.DedupeReservationState, error) {
	reservation, err := m.BeginPublicationWithResult(ctx, topo, key, reservationID, ttl)
	return reservation.State, err
}

func (m *memoryTransport) BeginPublicationWithResult(ctx context.Context, topo transport.Topology, key, reservationID string, ttl time.Duration) (transport.PublicationReservation, error) {
	if err := ctx.Err(); err != nil {
		return transport.PublicationReservation{}, err
	}
	if err := m.ValidateTopology(topo); err != nil {
		return transport.PublicationReservation{}, err
	}
	if key == "" || reservationID == "" || ttl <= 0 {
		return transport.PublicationReservation{}, fmt.Errorf("memory: dedupe key, reservation ID, and positive TTL required")
	}
	storageKey := memoryDedupeKey(topo, key)
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return transport.PublicationReservation{}, errMemoryClosed
	}
	if record, ok := m.dedupes[storageKey]; ok && now.Before(record.expires) {
		if record.state == transport.DedupeReservationPublished {
			return transport.PublicationReservation{State: transport.DedupeReservationPublished, MessageID: record.owner}, nil
		}
		return transport.PublicationReservation{State: transport.DedupeReservationPending, MessageID: record.owner}, nil
	}
	m.dedupes[storageKey] = memoryDedupe{expires: now.Add(ttl), owner: reservationID, state: transport.DedupeReservationPending}
	return transport.PublicationReservation{State: transport.DedupeReservationAcquired, MessageID: reservationID}, nil
}

func (m *memoryTransport) ConfirmPublication(ctx context.Context, topo transport.Topology, key, reservationID string) error {
	return m.updatePublication(ctx, topo, key, reservationID, true)
}

func (m *memoryTransport) AbortPublication(ctx context.Context, topo transport.Topology, key, reservationID string) error {
	return m.updatePublication(ctx, topo, key, reservationID, false)
}

func (m *memoryTransport) updatePublication(ctx context.Context, topo transport.Topology, key, reservationID string, confirm bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	storageKey := memoryDedupeKey(topo, key)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	record, ok := m.dedupes[storageKey]
	if !ok {
		if !confirm {
			return nil
		}
		return transport.ErrDedupeAmbiguous
	}
	if record.owner != reservationID || record.state != transport.DedupeReservationPending {
		return transport.ErrDedupeAmbiguous
	}
	if confirm {
		record.state = transport.DedupeReservationPublished
		m.dedupes[storageKey] = record
	} else {
		delete(m.dedupes, storageKey)
	}
	return nil
}

func (m *memoryTransport) List(ctx context.Context, topology transport.Topology, offset, limit int) ([]transport.DeadLetter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || limit <= 0 {
		return nil, fmt.Errorf("memory: invalid DLQ page")
	}
	m.mu.Lock()
	dlq := m.dlqs[topology.Name()]
	if offset >= len(dlq) {
		m.mu.Unlock()
		return nil, nil
	}
	end := min(len(dlq), offset+limit)
	result := append([]transport.DeadLetter(nil), dlq[offset:end]...)
	m.mu.Unlock()
	for index := range result {
		result[index].Message = cloneMessage(result[index].Message)
	}
	return result, nil
}

func (m *memoryTransport) Requeue(ctx context.Context, topology transport.Topology, messageIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	selected := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		selected[id] = struct{}{}
	}
	name := topology.Name()
	records := m.dlqs[name]
	type requeueRecord struct {
		message transport.Message
		groups  []string
	}
	requeue := make([]requeueRecord, 0, len(messageIDs))
	topo := m.ensureTopologyLocked(topology)
	for _, record := range records {
		_, byAdminID := selected[record.ID]
		_, byLegacyID := selected[record.Message.ID]
		if byAdminID || byLegacyID {
			groups := topo.orderedGroups
			if topology.Kind == transport.EventTopology && record.SourceGroup != "" {
				groups = []string{record.SourceGroup}
			}
			requeue = append(requeue, requeueRecord{message: record.Message, groups: groups})
		}
	}
	limit := memoryOption(topology, "memory.max_ready_entries", defaultMaxReadyEntries)
	additions := make(map[string]int)
	for _, record := range requeue {
		for _, groupName := range record.groups {
			additions[groupName]++
		}
	}
	for groupName, count := range additions {
		group := topo.groups[groupName]
		if group == nil {
			return fmt.Errorf("memory: DLQ source group %q is not provisioned", groupName)
		}
		current := len(group.ready) + len(group.inFlight)
		if current+count > limit {
			return memoryCapacityError(topology, "ready", current, limit)
		}
	}
	remaining := make([]transport.DeadLetter, 0, len(records)-len(requeue))
	for _, record := range records {
		_, byAdminID := selected[record.ID]
		_, byLegacyID := selected[record.Message.ID]
		if byAdminID || byLegacyID {
			continue
		}
		remaining = append(remaining, record)
	}
	for _, record := range requeue {
		for _, groupName := range record.groups {
			group := topo.groups[groupName]
			group.ready = append(group.ready, memoryQueued{message: record.message, readyAt: time.Now()})
			signal(group.notify)
		}
	}
	m.dlqs[name] = remaining
	return nil
}

func (m *memoryTransport) Purge(ctx context.Context, topology transport.Topology) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	delete(m.dlqs, topology.Name())
	return nil
}

func (m *memoryTransport) Count(ctx context.Context, topology transport.Topology) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.dlqs[topology.Name()])), nil
}

func (m *memoryTransport) Stats(ctx context.Context, topology transport.Topology) (transport.TopologyStats, error) {
	if err := ctx.Err(); err != nil {
		return transport.TopologyStats{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := transport.TopologyStats{Delayed: int64(m.delayedCounts[topology.Name()]), DLQ: int64(len(m.dlqs[topology.Name()])), LagKnown: true}
	oldest := time.Time{}
	if topo := m.topologies[topology.Name()]; topo != nil {
		for _, group := range topo.groups {
			stats.Pending += int64(len(group.ready))
			stats.InFlight += int64(len(group.inFlight))
			if len(group.ready) > 0 && (oldest.IsZero() || group.ready[0].readyAt.Before(oldest)) {
				oldest = group.ready[0].readyAt
			}
			for _, active := range group.inFlight {
				if oldest.IsZero() || active.queued.readyAt.Before(oldest) {
					oldest = active.queued.readyAt
				}
			}
		}
	}
	stats.Lag = stats.Pending + stats.InFlight
	stats.OldestAgeKnown = stats.Lag == 0 || !oldest.IsZero()
	if !oldest.IsZero() {
		stats.OldestAge = max(0, time.Since(oldest))
	}
	return stats, nil
}

func (m *memoryTransport) SubscriptionStats(ctx context.Context, sub transport.Subscription) (transport.TopologyStats, error) {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return transport.TopologyStats{}, fmt.Errorf("memory: event subscriber required")
	}
	if err := ctx.Err(); err != nil {
		return transport.TopologyStats{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	topo := m.topologies[sub.Topology.Name()]
	if topo == nil || topo.groups[sub.Group] == nil {
		return transport.TopologyStats{}, fmt.Errorf("memory: subscriber group %q is not provisioned", sub.Group)
	}
	group := topo.groups[sub.Group]
	pending, inFlight := int64(len(group.ready)), int64(len(group.inFlight))
	return transport.TopologyStats{Pending: pending, InFlight: inFlight, Lag: pending + inFlight, LagKnown: true}, nil
}

func (m *memoryTransport) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMemoryClosed
	}
	return nil
}

func (m *memoryTransport) Close(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		m.cancel()
		for _, topo := range m.topologies {
			for _, group := range topo.groups {
				signal(group.notify)
			}
		}
	}
	done := m.done
	m.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type memoryDelivery struct {
	transport    *memoryTransport
	msg          transport.Message
	topo         transport.Topology
	topologyName string
	group        string
	id           uint64
	count        int
	settled      transport.SettlementGuard
}

func (d *memoryDelivery) Message() transport.Message { return cloneMessage(d.msg) }
func (d *memoryDelivery) DeliveryAttempt() int       { return d.count }
func (d *memoryDelivery) Heartbeat(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return transport.ErrNotSupported
}

func (d *memoryDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		m := d.transport
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.closed {
			return errMemoryClosed
		}
		if !m.sourceActiveLocked(d) {
			return transport.ErrDeliverySettled
		}
		switch decision.Kind {
		case transport.DecisionAck:
			m.ackSourceLocked(d)
			return nil
		case transport.DecisionRetry:
			if len(decision.Next.Payload) == 0 || decision.RetryAt.IsZero() {
				return fmt.Errorf("memory: retry payload and deadline required")
			}
			next := cloneMessage(decision.Next)
			if decision.RetryAt.After(time.Now()) {
				if disabled, _ := d.topo.Options["eventbus.delay_disabled"].(bool); disabled {
					return transport.ErrNotSupported
				}
				return m.scheduleLocked(d.topo, []string{d.group}, next, decision.RetryAt, d)
			}
			topo := m.ensureTopologyLocked(d.topo)
			return m.enqueueLocked(topo, []string{d.group}, next, time.Now(), "ready", d)
		case transport.DecisionDeadLetter:
			name := d.topologyName
			limit := memoryOption(d.topo, "memory.max_dlq_entries", defaultMaxDLQEntries)
			if len(m.dlqs[name]) >= limit {
				return memoryCapacityError(d.topo, "dlq", len(m.dlqs[name]), limit)
			}
			msg := d.msg
			if decision.Source != nil {
				msg = cloneMessage(*decision.Source)
			}
			failedAt := decision.FailedAt
			if failedAt.IsZero() {
				failedAt = time.Now()
			}
			reason, lastError := transport.NormalizeDecisionMetadata(decision)
			if len(msg.Payload)+len(reason)+len(lastError) > transport.MaxEncodedDLQRecordBytes {
				return fmt.Errorf("memory: encoded DLQ record exceeds %d bytes", transport.MaxEncodedDLQRecordBytes)
			}
			m.dlqSeq++
			m.dlqs[name] = append(m.dlqs[name], transport.DeadLetter{
				ID: strconv.FormatUint(m.dlqSeq, 10), Message: msg,
				Reason: reason, LastError: lastError, Attempts: decision.Attempts, FailedAt: failedAt,
				Source: d.topologyName, SourceGroup: d.group, SourcePosition: strconv.FormatUint(d.id, 10),
			})
			m.ackSourceLocked(d)
			return nil
		default:
			return transport.ErrInvalidDecision
		}
	})
}

func (m *memoryTransport) sourceActiveLocked(delivery *memoryDelivery) bool {
	topo := m.topologies[delivery.topologyName]
	if topo == nil || topo.groups[delivery.group] == nil {
		return false
	}
	_, ok := topo.groups[delivery.group].inFlight[delivery.id]
	return ok
}

func (m *memoryTransport) ackSourceLocked(delivery *memoryDelivery) {
	if topo := m.topologies[delivery.topologyName]; topo != nil {
		if group := topo.groups[delivery.group]; group != nil {
			delete(group.inFlight, delivery.id)
			signal(group.notify)
			m.signalSchedulerLocked()
		}
	}
}

func memoryOption(topology transport.Topology, key string, fallback int) int {
	if value, ok := topology.Options[key].(int); ok && value > 0 {
		return value
	}
	return fallback
}

func memoryCapacityError(topology transport.Topology, resource string, current, limit int) error {
	return &transport.CapacityError{
		Transport: "memory", Topology: topology.Name(), Resource: resource,
		Current: int64(current), Limit: int64(limit),
	}
}

func cloneMessage(message transport.Message) transport.Message {
	message.Payload = append([]byte(nil), message.Payload...)
	if message.Headers != nil {
		headers := make(map[string]string, len(message.Headers))
		for key, value := range message.Headers {
			headers[key] = value
		}
		message.Headers = headers
	}
	return message
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (m *memoryTransport) signalSchedulerLocked() {
	signal(m.wake)
}
