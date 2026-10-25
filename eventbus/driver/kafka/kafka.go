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
	dlqLease      func(context.Context, transport.Topology) (*dlqAdminLease, error)

	mu                sync.Mutex
	sweepers          map[string]*delaySchedulerHandle
	schedulerStatuses map[string]map[string]*transport.SchedulerStatus
}

var (
	_ transport.Transport               = (*kafkaTransport)(nil)
	_ transport.TopologyValidator       = (*kafkaTransport)(nil)
	_ transport.DLQAdmin                = (*kafkaTransport)(nil)
	_ transport.StatsReader             = (*kafkaTransport)(nil)
	_ transport.SubscriptionStatsReader = (*kafkaTransport)(nil)
	_ transport.SubscriptionProvisioner = (*kafkaTransport)(nil)
	_ transport.SchedulerHealthReader   = (*kafkaTransport)(nil)
	_ transport.Delivery                = (*kafkaDelivery)(nil)
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
		schedulerStatuses: make(map[string]map[string]*transport.SchedulerStatus),
	}
	t.scanDLQ = t.scanDLQPartition
	return t
}

func (t *kafkaTransport) Name() string { return "kafka" }

func (t *kafkaTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		Durability:     transport.DurabilityBroker,
		Delay:          transport.DelayCapabilities{Supported: true, Precision: 5 * time.Second, Provisioning: transport.DelayTieredPerTopology, Optional: true, Tiers: len(delayTiers)},
		Order:          transport.OrderingCapabilities{Scope: transport.OrderingPartition, Coordination: transport.ScopeBroker},
		Settlement:     transport.SettlementCapabilities{Retry: transport.SettlementPublishBeforeSource, DLQ: transport.SettlementPublishBeforeSource},
		Admin:          transport.AdminCapabilities{DLQ: true, Stats: true, Pause: transport.ScopeProcess},
		Scheduler:      transport.SchedulerCapabilities{Health: true, WorkerScope: transport.ScopeBroker},
		Payload:        transport.PayloadCapabilities{CoreLimitSupported: true},
		DelayPrecision: 5 * time.Second,
		// Ordering is preserved only within one physical Kafka topic-partition.
		// Related main, delay, and private topics are independent logs.
		Ordering: true,
	}
}

func (t *kafkaTransport) mainTopic(topo transport.Topology) string {
	return topo.Name()
}

func (t *kafkaTransport) delayTopic(topo transport.Topology, tier time.Duration) string {
	return fmt.Sprintf("%s.delay-%s", topo.Name(), tier.String())
}

func (t *kafkaTransport) targetedDelayTopic(topo transport.Topology, tier time.Duration) string {
	return fmt.Sprintf("%s.target-delay-%s", topo.Name(), tier.String())
}

func (t *kafkaTransport) privateReadyTopic(topo transport.Topology, group string) string {
	return kafkaConsumerGroup(topo, group) + ".retry-ready"
}

func (t *kafkaTransport) dlqTopic(topo transport.Topology) string {
	return topo.Name() + ".dlq"
}

func (t *kafkaTransport) dlqAdminLockTopic(topo transport.Topology) string {
	return topo.Name() + ".dlq-admin-lock"
}

func (t *kafkaTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	if err := requireTargetedEventLayout(topo); err != nil {
		return err
	}
	main := t.mainTopic(topo)
	dlq := t.dlqTopic(topo)
	topics := []string{main, dlq}
	if topologyDelayEnabled(topo) {
		for _, tier := range delayTiers {
			topics = append(topics, t.delayTopic(topo, tier))
			if topo.Kind == transport.EventTopology {
				topics = append(topics, t.targetedDelayTopic(topo, tier))
			}
		}
	}
	lockTopic := t.dlqAdminLockTopic(topo)
	controlTopics := []string{lockTopic}
	layoutTopic := ""
	if topo.Kind == transport.EventTopology {
		layoutTopic = t.eventLayoutTopic(topo)
		controlTopics = append(controlTopics, layoutTopic)
	}

	partitions := int32(1)
	if val, ok := topo.Options["kafka.partitions"].(int32); ok && val > 0 {
		partitions = val
	}

	replFactor := int16(1)
	if val, ok := topo.Options["kafka.replication_factor"].(int16); ok && val > 0 {
		replFactor = val
	}

	configs := topicConfigs(topo)
	kadmConfigs := pointerConfigs(configs)

	resp, err := t.adm.CreateTopics(ctx, partitions, replFactor, kadmConfigs, topics...)
	if err != nil {
		return fmt.Errorf("kafka EnsureTopology fatal: %w", err)
	}
	for _, tr := range resp {
		if tr.Err != nil && !errors.Is(tr.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka EnsureTopology CreateTopic error for %q: %w", tr.Topic, tr.Err)
		}
	}
	controlResp, err := t.adm.CreateTopics(ctx, 1, replFactor, kadmConfigs, controlTopics...)
	if err != nil {
		return fmt.Errorf("kafka EnsureTopology control topics: %w", err)
	}
	for _, tr := range controlResp {
		if tr.Err != nil && !errors.Is(tr.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka EnsureTopology CreateTopic error for %q: %w", tr.Topic, tr.Err)
		}
	}
	if topo.Kind == transport.EventTopology {
		if err := t.validateRelatedTopicPartitions(ctx, []string{layoutTopic}, 1); err != nil {
			var mismatch *topicPartitionMismatchError
			if errors.As(err, &mismatch) {
				return fmt.Errorf("kafka: targeted event layout topic %q must have exactly 1 partition and Kafka cannot reduce partitions; use a new EventBus namespace or delete only this topic in disposable infrastructure: %w", layoutTopic, err)
			}
			return err
		}
	}
	if err := t.validateRelatedTopicPartitions(ctx, topics, partitions); err != nil {
		return err
	}
	if err := t.reconcileRelatedTopicConfigs(ctx, append(topics, controlTopics...), replFactor, configs); err != nil {
		return err
	}
	if err := t.validateRelatedTopicPartitions(ctx, []string{lockTopic}, 1); err != nil {
		return err
	}
	if err := t.reconcileRelatedTopicConfigs(ctx, []string{lockTopic}, replFactor, configs); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	topoName := topo.Name()
	if topologyDelayEnabled(topo) {
		if _, ok := t.sweepers[topoName]; ok {
			return nil
		}
		scheduler, err := t.startDelayScheduler(topo)
		if err != nil {
			return fmt.Errorf("kafka EnsureTopology delay scheduler: %w", err)
		}
		t.sweepers[topoName] = scheduler
	}

	return nil
}

func topicConfigs(topo transport.Topology) map[string]string {
	configs := make(map[string]string)
	if values, ok := topo.Options["kafka.topic_configs"].(map[string]string); ok {
		for key, value := range values {
			configs[key] = value
		}
	}
	if value, ok := topo.Options["kafka.retention_ms"].(time.Duration); ok && value > 0 {
		configs["retention.ms"] = fmt.Sprintf("%d", value.Milliseconds())
	}
	return configs
}

func pointerConfigs(configs map[string]string) map[string]*string {
	if len(configs) == 0 {
		return nil
	}
	result := make(map[string]*string, len(configs))
	for key, value := range configs {
		value := value
		result[key] = &value
	}
	return result
}

func (t *kafkaTransport) EnsureSubscription(ctx context.Context, sub transport.Subscription) error {
	if err := t.ValidateTopology(sub.Topology); err != nil {
		return err
	}
	if sub.Topology.Kind == transport.EventTopology && sub.Group == "" {
		return fmt.Errorf("kafka: event subscription group must not be blank")
	}
	if sub.Topology.Kind == transport.TaskTopology {
		if sub.Group != "" {
			return fmt.Errorf("kafka: task subscription group must be empty")
		}
		return nil
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return err
	}
	topic := t.privateReadyTopic(sub.Topology, sub.Group)
	if len(topic) > 249 || !kafkaTopicName.MatchString(topic) {
		return fmt.Errorf("kafka: subscriber produces invalid private retry topic name %q", topic)
	}
	partitions := int32(1)
	if value, ok := sub.Topology.Options["kafka.partitions"].(int32); ok {
		partitions = value
	}
	replicationFactor := int16(1)
	if value, ok := sub.Topology.Options["kafka.replication_factor"].(int16); ok {
		replicationFactor = value
	}
	configs := topicConfigs(sub.Topology)
	marker := t.subscriberMarkerTopic(sub.Topology, sub.Group)
	response, err := t.adm.CreateTopics(ctx, partitions, replicationFactor, pointerConfigs(configs), topic)
	if err != nil {
		return fmt.Errorf("kafka: ensure subscriber retry topic: %w", err)
	}
	for _, result := range response {
		if result.Err != nil && !errors.Is(result.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: ensure subscriber retry topic %q: %w", result.Topic, result.Err)
		}
	}
	if err := t.validateRelatedTopicPartitions(ctx, []string{topic}, partitions); err != nil {
		return err
	}
	if err := t.reconcileRelatedTopicConfigs(ctx, []string{topic}, replicationFactor, configs); err != nil {
		return err
	}
	response, err = t.adm.CreateTopics(ctx, 1, replicationFactor, pointerConfigs(subscriberMarkerConfigs()), marker)
	if err != nil {
		return fmt.Errorf("kafka: ensure subscriber registry marker: %w", err)
	}
	for _, result := range response {
		if result.Err != nil && !errors.Is(result.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: ensure subscriber registry marker %q: %w", result.Topic, result.Err)
		}
	}
	if err := t.validateRelatedTopicPartitions(ctx, []string{marker}, 1); err != nil {
		return err
	}
	if err := t.reconcileRelatedTopicConfigs(ctx, []string{marker}, replicationFactor, subscriberMarkerConfigs()); err != nil {
		return err
	}
	return t.validateProvisionedSubscriber(ctx, sub.Topology, sub.Group)
}

func (t *kafkaTransport) reconcileRelatedTopicConfigs(ctx context.Context, topics []string, replicationFactor int16, desired map[string]string) error {
	backoff := 25 * time.Millisecond
	for {
		ready, err := t.relatedTopicReplicationReady(ctx, topics, replicationFactor)
		if err != nil {
			return err
		}
		if ready {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("kafka: wait for related topic replication metadata: %w", ctx.Err())
		case <-timer.C:
		}
		backoff = min(250*time.Millisecond, backoff*2)
	}
	configs, err := t.adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return fmt.Errorf("kafka: describe related topic configs: %w", err)
	}
	byTopic := make(map[string]map[string]string, len(configs))
	for _, resource := range configs {
		if resource.Err != nil {
			return fmt.Errorf("kafka: describe topic %q configs: %w", resource.Name, resource.Err)
		}
		values := make(map[string]string, len(resource.Configs))
		for _, config := range resource.Configs {
			if config.Value != nil {
				values[config.Key] = *config.Value
			}
		}
		byTopic[resource.Name] = values
	}
	for _, topic := range topics {
		current := byTopic[topic]
		if policy := current["cleanup.policy"]; policy != "" && !cleanupPolicyDeleteOnly(policy) {
			return fmt.Errorf("kafka: related topic %q has unsafe cleanup.policy %q", topic, policy)
		}
		changes := make([]kadm.AlterConfig, 0, len(desired))
		for key, value := range desired {
			if current[key] != value {
				value := value
				changes = append(changes, kadm.AlterConfig{Name: key, Value: &value})
			}
		}
		if len(changes) == 0 {
			continue
		}
		responses, err := t.adm.AlterTopicConfigs(ctx, changes, topic)
		if err != nil {
			return fmt.Errorf("kafka: alter topic %q configs: %w", topic, err)
		}
		if _, err := responses.On(topic, func(response *kadm.AlterConfigsResponse) error { return response.Err }); err != nil {
			return fmt.Errorf("kafka: alter topic %q configs: %w", topic, err)
		}
	}
	return nil
}

func (t *kafkaTransport) relatedTopicReplicationReady(ctx context.Context, topics []string, replicationFactor int16) (bool, error) {
	metadata, err := t.adm.Metadata(ctx, topics...)
	if err != nil {
		if isKafkaLeaderTransition(err) {
			return false, nil
		}
		return false, fmt.Errorf("kafka: inspect related topic replication: %w", err)
	}
	for _, topic := range topics {
		detail, ok := metadata.Topics[topic]
		if !ok || isKafkaLeaderTransition(detail.Err) {
			return false, nil
		}
		if detail.Err != nil {
			return false, fmt.Errorf("kafka: inspect related topic %q: %w", topic, detail.Err)
		}
		for partition, partitionDetail := range detail.Partitions {
			if isKafkaLeaderTransition(partitionDetail.Err) || partitionDetail.Leader < 0 {
				return false, nil
			}
			if partitionDetail.Err != nil {
				return false, fmt.Errorf("kafka: inspect related topic %q partition %d: %w", topic, partition, partitionDetail.Err)
			}
			if got := int16(len(partitionDetail.Replicas)); got != replicationFactor {
				return false, fmt.Errorf("kafka: related topic %q partition %d replication factor is %d, want %d", topic, partition, got, replicationFactor)
			}
		}
	}
	return true, nil
}

func (t *kafkaTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	if err := requireTargetedEventLayout(topo); err != nil {
		return err
	}
	removeMessageHeader(&msg, kafkaTargetGroupHeader)
	topic := t.mainTopic(topo)

	return t.produceConfirmed(ctx, messageRecord(topic, msg))
}

func (t *kafkaTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	if err := requireTargetedEventLayout(topo); err != nil {
		return err
	}
	removeMessageHeader(&msg, kafkaTargetGroupHeader)
	if !topologyDelayEnabled(topo) && runAt.After(time.Now()) {
		return fmt.Errorf("%w: kafka delay is disabled for topology %q", transport.ErrNotSupported, topo.Name())
	}
	topic := t.delayDestination(topo, runAt, time.Now())
	if topic == t.mainTopic(topo) {
		return t.produceConfirmed(ctx, messageRecord(topic, msg))
	}
	return t.produceConfirmed(ctx, scheduledMessageRecord(topic, msg, runAt))
}

func (t *kafkaTransport) publishTargetedAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	if err := requireTargetedEventLayout(topo); err != nil {
		return err
	}
	group := msg.Headers[kafkaTargetGroupHeader]
	if group == "" {
		return fmt.Errorf("kafka: targeted retry group required")
	}
	if !topologyDelayEnabled(topo) && runAt.After(time.Now()) {
		return fmt.Errorf("%w: kafka delay is disabled for topology %q", transport.ErrNotSupported, topo.Name())
	}
	if err := t.validateProvisionedSubscriber(ctx, topo, group); err != nil {
		return err
	}
	topic := t.targetedDelayDestination(topo, group, runAt, time.Now())
	if topic == t.privateReadyTopic(topo, group) {
		removeMessageHeader(&msg, kafkaTargetGroupHeader)
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
	topics := []string{t.mainTopic(sub.Topology)}
	if sub.Topology.Kind == transport.EventTopology {
		if err := t.EnsureSubscription(ctx, sub); err != nil {
			return err
		}
		topics = append(topics, t.privateReadyTopic(sub.Topology, sub.Group))
	}
	group := kafkaConsumerGroup(sub.Topology, sub.Group)

	opts := mainConsumerOptions(topics, group, sub.Topology)
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
	return t.consumeRecords(ctx, sub, cl, coordinator, rebalances, capacity, deliver)
}

func kafkaConsumerGroup(topo transport.Topology, group string) string {
	if version, _ := topo.Options[transport.ResourceNameVersionOption].(string); version == transport.ResourceNameVersionLegacy {
		name := topo.Name() + "-group"
		if group != "" {
			name = topo.Name() + "-" + group
		}
		return name
	}
	return topo.ConsumerName(group)
}

func mainConsumerOptions(topics []string, group string, topo transport.Topology) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(), // Bug K1 mitigation
	}
	return append(opts, topologyFetchOptions(topo)...)
}

func (t *kafkaTransport) consumeRecords(ctx context.Context, sub transport.Subscription, cl recordConsumer, coordinator *offsetCoordinator, rebalances *rebalanceController, capacity int, deliver func(transport.Delivery)) error {
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

		rewind, err := t.dispatchBatch(ctx, sub.Topology, sub.Group, coordinator, rebalances, fetches.Records(), deliver)
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

func (t *kafkaTransport) targetedDelayTopics(topo transport.Topology) []string {
	topics := make([]string, 0, len(delayTiers))
	for _, tier := range delayTiers {
		topics = append(topics, t.targetedDelayTopic(topo, tier))
	}
	return topics
}

func topologyDelayEnabled(topo transport.Topology) bool {
	if disabled, _ := topo.Options["eventbus.delay_disabled"].(bool); disabled {
		return false
	}
	disabled, _ := topo.Options["kafka.delay_disabled"].(bool)
	return !disabled
}

func topologyFetchOptions(topo transport.Topology) []kgo.Opt {
	opts := make([]kgo.Opt, 0, 6)
	if val, ok := topo.Options["kafka.fetch_max_wait"].(time.Duration); ok && val > 0 {
		opts = append(opts, kgo.FetchMaxWait(val))
	}
	if val, ok := topo.Options["kafka.fetch_min_bytes"].(int32); ok && val > 0 {
		opts = append(opts, kgo.FetchMinBytes(val))
	}
	if val, ok := topo.Options["kafka.fetch_max_bytes"].(int32); ok && val > 0 {
		opts = append(opts, kgo.FetchMaxBytes(val))
	}
	if val, ok := topo.Options["kafka.fetch_max_partition_bytes"].(int32); ok && val > 0 {
		opts = append(opts, kgo.FetchMaxPartitionBytes(val))
	}
	if val, ok := topo.Options["kafka.max_buffered_records"].(int); ok && val > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(val))
	}
	if val, ok := topo.Options["kafka.max_buffered_bytes"].(int); ok && val > 0 {
		opts = append(opts, kgo.MaxBufferedBytes(val))
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
	group       string
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
			next := decision.Next
			var err error
			if d.topo.Kind == transport.EventTopology {
				setMessageHeader(&next, kafkaTargetGroupHeader, d.group)
				err = d.transport.publishTargetedAt(ctx, d.topo, next, decision.RetryAt)
			} else {
				err = d.transport.PublishAt(ctx, d.topo, next, decision.RetryAt)
			}
			if err != nil {
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
