package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	registry.Register("kafka", newDefaultTransport)
}

func newDefaultTransport() (transport.Transport, error) {
	c, ok := commonkafka.LookupDefault()
	if !ok {
		return nil, fmt.Errorf("kafka default client not set")
	}
	return NewWithClient(c)
}

var delayTiers = []time.Duration{
	5 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
}

type kafkaTransport struct {
	cl            *kgo.Client
	adm           kafkaAdmin
	factory       *commonkafka.NativeClientFactory
	producerOwned bool
	producer      recordProducer
	producerErr   error
	scanDLQ       dlqPartitionScanner
	dlqAdminMu    sync.Mutex

	mu       sync.Mutex
	sweepers map[string]*delaySchedulerHandle
}

var (
	_ transport.Transport         = (*kafkaTransport)(nil)
	_ transport.TopologyValidator = (*kafkaTransport)(nil)
	_ transport.DLQAdmin          = (*kafkaTransport)(nil)
	_ transport.StatsReader       = (*kafkaTransport)(nil)
	_ transport.Delivery          = (*kafkaDelivery)(nil)
)

// New creates a plaintext compatibility transport around base.
//
// Deprecated: use NewWithClient or NewWithConfig so internal consumers inherit
// TLS, SASL, client ID, timeout, and broker configuration.
func New(base *kgo.Client, brokers []string) transport.Transport {
	overrides := make([]kgo.Opt, 0, 1)
	if len(brokers) > 0 {
		overrides = append(overrides, kgo.SeedBrokers(brokers...))
	}
	factory, _ := commonkafka.NewNativeClientFactoryFromClient(base, overrides...)
	return newKafkaTransport(base, factory, false)
}

// NewWithClient creates a transport whose producer/admin use the connected
// wrapper client and whose internal consumers inherit its complete config.
// The caller retains ownership of client.
func NewWithClient(client *commonkafka.Client) (transport.Transport, error) {
	if client == nil {
		return nil, fmt.Errorf("kafka: client is nil")
	}
	base := client.Native()
	if base == nil {
		return nil, fmt.Errorf("kafka: client is not connected")
	}
	if err := validateSettlementProducer(base); err != nil {
		return nil, err
	}
	cfg := client.Config()
	factory, err := commonkafka.NewNativeClientFactory(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client factory: %w", err)
	}
	return newKafkaTransport(base, factory, false), nil
}

// NewWithConfig creates a self-contained transport from cfg. All Kafka clients
// share the same immutable connection/security snapshot. Close owns the base
// producer created by this constructor.
func NewWithConfig(cfg commonkafka.Config) (transport.Transport, error) {
	if cfg.ProducerAcks == 0 {
		return nil, fmt.Errorf("%w: kafka EventBus requires broker-acknowledged production (ProducerAcks=-1 or 1)", transport.ErrNotSupported)
	}
	factory, err := commonkafka.NewNativeClientFactory(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client factory: %w", err)
	}
	producer, err := factory.NewProducer()
	if err != nil {
		return nil, fmt.Errorf("kafka: create producer: %w", err)
	}
	return newKafkaTransport(producer, factory, true), nil
}

func newKafkaTransport(base *kgo.Client, factory *commonkafka.NativeClientFactory, producerOwned bool) *kafkaTransport {
	t := &kafkaTransport{
		cl: base, adm: kadm.NewClient(base), factory: factory,
		producer: base, producerErr: validateSettlementProducer(base),
		producerOwned: producerOwned, sweepers: make(map[string]*delaySchedulerHandle),
	}
	t.scanDLQ = t.scanDLQPartition
	return t
}

func (t *kafkaTransport) Name() string { return "kafka" }

func (t *kafkaTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		DelayPrecision: 5 * time.Second, // Based on the smallest tier
		// Ordering keys map to Kafka partitions. Dispatch, commits, and
		// ownership transfer are serialized within each partition.
		Ordering: true,
	}
}

func (t *kafkaTransport) mainTopic(topo transport.Topology) string {
	return topo.Name()
}

func (t *kafkaTransport) delayTopic(topo transport.Topology, tier time.Duration) string {
	return fmt.Sprintf("%s.delay-%s", topo.Name(), tier.String())
}

func (t *kafkaTransport) dlqTopic(topo transport.Topology) string {
	return topo.Name() + ".dlq"
}

func (t *kafkaTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	main := t.mainTopic(topo)
	dlq := t.dlqTopic(topo)
	topics := []string{main, dlq}
	for _, tier := range delayTiers {
		topics = append(topics, t.delayTopic(topo, tier))
	}

	partitions := int32(1)
	if val, ok := topo.Options["kafka.partitions"].(int32); ok && val > 0 {
		partitions = val
	}

	replFactor := int16(1)
	if val, ok := topo.Options["kafka.replication_factor"].(int16); ok && val > 0 {
		replFactor = val
	}

	var kadmConfigs map[string]*string

	// Collect configs
	configs := make(map[string]string)
	if val, ok := topo.Options["kafka.topic_configs"].(map[string]string); ok {
		for k, v := range val {
			configs[k] = v
		}
	}
	if val, ok := topo.Options["kafka.retention_ms"].(time.Duration); ok && val > 0 {
		configs["retention.ms"] = fmt.Sprintf("%d", val.Milliseconds())
	}

	// Convert to map[string]*string for kadm
	if len(configs) > 0 {
		kadmConfigs = make(map[string]*string, len(configs))
		for k, v := range configs {
			v := v // capture loop variable
			kadmConfigs[k] = &v
		}
	}

	resp, err := t.adm.CreateTopics(ctx, partitions, replFactor, kadmConfigs, topics...)
	if err != nil {
		return fmt.Errorf("kafka EnsureTopology fatal: %w", err)
	}
	for _, tr := range resp {
		if tr.Err != nil && !errors.Is(tr.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka EnsureTopology CreateTopic error for %q: %w", tr.Topic, tr.Err)
		}
	}
	if err := t.validateRelatedTopicPartitions(ctx, topics, partitions); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	topoName := topo.Name()
	if _, ok := t.sweepers[topoName]; !ok {
		scheduler, err := t.startDelayScheduler(topo)
		if err != nil {
			return fmt.Errorf("kafka EnsureTopology delay scheduler: %w", err)
		}
		t.sweepers[topoName] = scheduler
	}

	return nil
}

func (t *kafkaTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	topic := t.mainTopic(topo)

	return t.produceConfirmed(ctx, messageRecord(topic, msg))
}

func (t *kafkaTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	topic := t.delayDestination(topo, runAt, time.Now())
	if topic == t.mainTopic(topo) {
		return t.produceConfirmed(ctx, messageRecord(topic, msg))
	}
	return t.produceConfirmed(ctx, scheduledMessageRecord(topic, msg, runAt))
}

func (t *kafkaTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	if t.producerErr != nil {
		return t.producerErr
	}
	capacity, err := subscriptionCapacity(sub)
	if err != nil {
		return err
	}
	topic := t.mainTopic(sub.Topology)
	group := sub.Topology.Name() + "-group"
	if sub.Group != "" {
		group = sub.Topology.Name() + "-" + sub.Group
	}

	opts := mainConsumerOptions(topic, group, sub.Topology)
	rebalances := newRebalanceController()
	opts = append(opts, rebalances.options()...)

	cl, err := t.newManualConsumer(opts...)
	if err != nil {
		return err
	}
	coordinator := newOffsetCoordinator(cl, capacity)
	rebalances.bind(coordinator)
	defer func() {
		if sub.WaitForHandlers != nil {
			sub.WaitForHandlers()
		}
		cl.CloseAllowingRebalance()
		coordinator.close()
	}()
	return t.consumeRecords(ctx, sub.Topology, cl, coordinator, rebalances, capacity, deliver)
}

func mainConsumerOptions(topic, group string, topo transport.Topology) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.BlockRebalanceOnPoll(), // Bug K1 mitigation
	}
	return append(opts, topologyFetchOptions(topo)...)
}

func (t *kafkaTransport) consumeRecords(ctx context.Context, topo transport.Topology, cl recordConsumer, coordinator *offsetCoordinator, rebalances *rebalanceController, capacity int, deliver func(transport.Delivery)) error {
	for {
		// Do not poll another batch until every record in this bounded batch
		// settles. PollRecords limits application-owned records, independently
		// of franz-go's byte-bounded internal fetch buffer.
		fetches := cl.PollRecords(ctx, capacity)
		if fetches.IsClientClosed() || fetches.Err() != nil {
			if errors.Is(fetches.Err(), context.Canceled) {
				return ctx.Err()
			}
			return fetches.Err()
		}

		rewind, err := t.dispatchBatch(ctx, topo, coordinator, rebalances, fetches.Records(), deliver)
		if err != nil && !errors.Is(err, errRebalanceRequested) {
			return err
		}
		if len(rewind) > 0 {
			// PollRecords has already advanced the local fetch position. Restore
			// the first undispatched record while BlockRebalanceOnPoll still owns
			// the assignment, so retained partitions cannot skip drained work.
			cl.SetOffsets(rewind)
		}
		// BlockRebalanceOnPoll keeps the fetched assignment valid until every
		// dispatched handler has settled. Only then may revoke/lost callbacks
		// fence ownership and let another member fetch these partitions.
		cl.AllowRebalance()
	}
}

func (t *kafkaTransport) delayTopics(topo transport.Topology) []string {
	topics := make([]string, 0, len(delayTiers))
	for _, tier := range delayTiers {
		topics = append(topics, t.delayTopic(topo, tier))
	}
	return topics
}

func topologyFetchOptions(topo transport.Topology) []kgo.Opt {
	opts := make([]kgo.Opt, 0, 2)
	if val, ok := topo.Options["kafka.fetch_max_wait"].(time.Duration); ok && val > 0 {
		opts = append(opts, kgo.FetchMaxWait(val))
	}
	if val, ok := topo.Options["kafka.fetch_max_bytes"].(int32); ok && val > 0 {
		opts = append(opts, kgo.FetchMaxBytes(val))
	}
	return opts
}

func (t *kafkaTransport) newConsumer(opts ...kgo.Opt) (*kgo.Client, error) {
	if t.factory == nil {
		return nil, fmt.Errorf("kafka: internal client factory is unavailable")
	}
	return t.factory.NewConsumer(opts...)
}

func (t *kafkaTransport) newManualConsumer(opts ...kgo.Opt) (*kgo.Client, error) {
	if t.factory == nil {
		return nil, fmt.Errorf("kafka: internal client factory is unavailable")
	}
	return t.factory.NewManualConsumer(opts...)
}

func (t *kafkaTransport) newReader(opts ...kgo.Opt) (*kgo.Client, error) {
	if t.factory == nil {
		return nil, fmt.Errorf("kafka: internal client factory is unavailable")
	}
	return t.factory.NewReader(opts...)
}

func (t *kafkaTransport) Ping(ctx context.Context) error { return t.cl.Ping(ctx) }
func (t *kafkaTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	handles := make([]*delaySchedulerHandle, 0, len(t.sweepers))
	for _, scheduler := range t.sweepers {
		scheduler.cancel()
		handles = append(handles, scheduler)
	}
	t.sweepers = make(map[string]*delaySchedulerHandle)
	owned := t.producerOwned
	t.producerOwned = false
	cl := t.cl
	t.mu.Unlock()
	var waitErr error
	for _, scheduler := range handles {
		select {
		case <-scheduler.done:
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	}
	if owned && cl != nil {
		cl.Close()
	}
	return waitErr
}

// kafkaDelivery implements Delivery
type kafkaDelivery struct {
	transport   *kafkaTransport
	topo        transport.Topology
	coordinator *offsetCoordinator
	record      *kgo.Record
	msg         transport.Message
	count       int
	settled     transport.SettlementGuard
	completed   func(error)
}

func (d *kafkaDelivery) Message() transport.Message { return d.msg }
func (d *kafkaDelivery) DeliveryAttempt() int       { return d.count }

func (d *kafkaDelivery) ack(ctx context.Context) error {
	return d.coordinator.settle(ctx, d.record)
}

func (d *kafkaDelivery) deadLetter(ctx context.Context, decision transport.Decision) error {
	msg := d.msg
	if decision.Source != nil {
		msg = *decision.Source
	}
	payload, err := d.deadLetterRecord(msg, decision)
	if err != nil {
		return err
	}
	dlqMessage := msg
	dlqMessage.Payload = payload
	err = d.transport.produceConfirmed(ctx, messageRecord(d.transport.dlqTopic(d.topo), dlqMessage))
	if err != nil {
		return err
	}
	return d.ack(ctx)
}

func (d *kafkaDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	err := d.settled.Settle(decision, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch decision.Kind {
		case transport.DecisionAck:
			return d.ack(ctx)
		case transport.DecisionRetry:
			if err := d.transport.PublishAt(ctx, d.topo, decision.Next, decision.RetryAt); err != nil {
				return err
			}
			return d.ack(ctx)
		case transport.DecisionDeadLetter:
			return d.deadLetter(ctx, decision)
		default:
			return transport.ErrInvalidDecision
		}
	})
	if d.completed != nil && !errors.Is(err, transport.ErrDeliverySettled) && !errors.Is(err, transport.ErrSettlementInProgress) {
		d.completed(err)
	}
	return err
}

func (d *kafkaDelivery) Heartbeat(ctx context.Context) error {
	return nil
}
