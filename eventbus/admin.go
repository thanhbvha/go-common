package eventbus

import (
	"context"
	"fmt"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

// TopologyRef explicitly selects a task or event topology for administration.
type TopologyRef struct {
	Kind TopologyKind
	Name string
}

func TaskRef(name string) TopologyRef  { return TopologyRef{Kind: TaskTopology, Name: name} }
func EventRef(name string) TopologyRef { return TopologyRef{Kind: EventTopology, Name: name} }

func (b *bus) adminTopology(ref TopologyRef) (Topology, error) {
	if err := validateName("topology name", ref.Name); err != nil {
		return Topology{}, err
	}
	if ref.Kind != TaskTopology && ref.Kind != EventTopology {
		return Topology{}, fmt.Errorf("%w: unknown topology kind %d", ErrInvalidOption, ref.Kind)
	}
	b.engine.mu.RLock()
	defer b.engine.mu.RUnlock()
	var topo Topology
	var ok bool
	if ref.Kind == TaskTopology {
		topo, ok = b.engine.taskTopologies[ref.Name]
	} else {
		topo, ok = b.engine.eventTopologies[ref.Name]
	}
	if !ok {
		return Topology{}, fmt.Errorf("%w: %s %q", ErrUndeclaredTopology, ref.Kind.String(), ref.Name)
	}
	return topo, nil
}

func (b *bus) adminEventTopology(ref TopologyRef, subscriber string) (Topology, error) {
	if ref.Kind != EventTopology || subscriber == "" {
		return b.adminTopology(ref)
	}
	if err := validateName("subscriber name", subscriber); err != nil {
		return Topology{}, err
	}
	b.engine.mu.RLock()
	defer b.engine.mu.RUnlock()
	topo, ok := b.engine.subTopologies[ref.Name][subscriber]
	if !ok {
		return Topology{}, fmt.Errorf("%w: event %q subscriber %q", ErrUndeclaredTopology, ref.Name, subscriber)
	}
	return topo, nil
}

func (b *bus) TopologyStats(ctx context.Context, ref TopologyRef) (transport.TopologyStats, error) {
	topo, err := b.adminTopology(ref)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	reader, ok := b.transport.(transport.StatsReader)
	if !ok {
		return transport.TopologyStats{}, fmt.Errorf("eventbus: stats: %w", ErrNotSupported)
	}
	stats, err := reader.Stats(ctx, topo)
	if err == nil {
		if metrics, ok := b.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.ObserveStats(metricsName(ref.Name, ""), stats)
		}
	}
	return stats, err
}

func (b *bus) SubscriberStats(ctx context.Context, event, subscriber string) (transport.TopologyStats, error) {
	topo, err := b.adminEventTopology(EventRef(event), subscriber)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	reader, ok := b.transport.(transport.SubscriptionStatsReader)
	if !ok {
		return transport.TopologyStats{}, fmt.Errorf("eventbus: subscriber stats: %w", ErrNotSupported)
	}
	stats, err := reader.SubscriptionStats(ctx, transport.Subscription{Topology: topo, Group: subscriber})
	if err == nil {
		if metrics, ok := b.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.ObserveStats(metricsName(event, subscriber), stats)
		}
	}
	return stats, err
}

func (b *bus) ListDeadLetters(ctx context.Context, ref TopologyRef, cursor string, limit int) (transport.DLQPage, error) {
	topo, err := b.adminTopology(ref)
	if err != nil {
		return transport.DLQPage{}, err
	}
	if cursorAdmin, ok := b.transport.(transport.CursorDLQAdmin); ok {
		return cursorAdmin.ListPage(ctx, topo, cursor, limit)
	}
	if cursor != "" {
		return transport.DLQPage{}, fmt.Errorf("eventbus: DLQ cursor pagination: %w", ErrNotSupported)
	}
	admin, ok := b.transport.(transport.DLQAdmin)
	if !ok {
		return transport.DLQPage{}, fmt.Errorf("eventbus: DLQ list: %w", ErrNotSupported)
	}
	records, err := admin.List(ctx, topo, 0, limit)
	return transport.DLQPage{Records: records}, err
}

func (b *bus) CountDeadLetters(ctx context.Context, ref TopologyRef) (int64, error) {
	topo, err := b.adminTopology(ref)
	if err != nil {
		return 0, err
	}
	admin, ok := b.transport.(transport.DLQAdmin)
	if !ok {
		return 0, fmt.Errorf("eventbus: DLQ count: %w", ErrNotSupported)
	}
	return admin.Count(ctx, topo)
}

func (b *bus) RequeueDeadLetters(ctx context.Context, ref TopologyRef, ids []string) error {
	topo, err := b.adminTopology(ref)
	if err != nil {
		return err
	}
	admin, ok := b.transport.(transport.DLQAdmin)
	if !ok {
		return fmt.Errorf("eventbus: DLQ requeue: %w", ErrNotSupported)
	}
	return admin.Requeue(ctx, topo, ids)
}

func (b *bus) PurgeDeadLetters(ctx context.Context, ref TopologyRef) error {
	topo, err := b.adminTopology(ref)
	if err != nil {
		return err
	}
	admin, ok := b.transport.(transport.DLQAdmin)
	if !ok {
		return fmt.Errorf("eventbus: DLQ purge: %w", ErrNotSupported)
	}
	return admin.Purge(ctx, topo)
}
