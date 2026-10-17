package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
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
	adm           *kadm.Client
	factory       *commonkafka.NativeClientFactory
	producerOwned bool

	mu       sync.Mutex
	sweepers map[string]context.CancelFunc
}

var (
	_ transport.Transport         = (*kafkaTransport)(nil)
	_ transport.TopologyValidator = (*kafkaTransport)(nil)
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
	factory, err := commonkafka.NewNativeClientFactory(client.Config())
	if err != nil {
		return nil, fmt.Errorf("kafka: create client factory: %w", err)
	}
	return newKafkaTransport(base, factory, false), nil
}

// NewWithConfig creates a self-contained transport from cfg. All Kafka clients
// share the same immutable connection/security snapshot. Close owns the base
// producer created by this constructor.
func NewWithConfig(cfg commonkafka.Config) (transport.Transport, error) {
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
	return &kafkaTransport{
		cl: base, adm: kadm.NewClient(base), factory: factory,
		producerOwned: producerOwned, sweepers: make(map[string]context.CancelFunc),
	}
}

func (t *kafkaTransport) Name() string { return "kafka" }

func (t *kafkaTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		DelayPrecision: 5 * time.Second, // Based on the smallest tier
		// The broker routes equal keys to one partition, but the current engine
		// processes deliveries concurrently and has no contiguous offset
		// coordinator. Do not advertise processing order until Phase P5.2-P5.3.
		Ordering: false,
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
		if tr.Err != nil && !strings.Contains(tr.Err.Error(), "TOPIC_ALREADY_EXISTS") {
			return fmt.Errorf("kafka EnsureTopology CreateTopic error for %q: %w", tr.Topic, tr.Err)
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	topoName := topo.Name()
	if _, ok := t.sweepers[topoName]; !ok {
		delayOpts := []kgo.Opt{
			kgo.ConsumerGroup(topo.Name() + "-delay-group"),
			kgo.ConsumeTopics(t.delayTopics(topo)...),
			kgo.AutoCommitMarks(),
			kgo.BlockRebalanceOnPoll(),
		}
		delayOpts = append(delayOpts, topologyFetchOptions(topo)...)
		cl, err := t.newConsumer(delayOpts...)
		if err != nil {
			return fmt.Errorf("kafka EnsureTopology delay consumer: %w", err)
		}
		sweepCtx, cancel := context.WithCancel(context.Background())
		t.sweepers[topoName] = cancel
		go t.delayConsumer(sweepCtx, topo, cl)
	}

	return nil
}

func (t *kafkaTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	topic := t.mainTopic(topo)

	var key []byte
	if msg.OrderingKey != "" {
		key = []byte(msg.OrderingKey)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: msg.Payload,
	}

	return t.cl.ProduceSync(ctx, record).FirstErr()
}

func (t *kafkaTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	delay := time.Until(runAt)
	if delay <= 0 {
		return t.Publish(ctx, topo, msg)
	}

	// Find suitable tier
	var targetTier time.Duration = delayTiers[0]
	for i := len(delayTiers) - 1; i >= 0; i-- {
		if delay >= delayTiers[i] {
			targetTier = delayTiers[i]
			break
		}
	}

	topic := t.delayTopic(topo, targetTier)

	var key []byte
	if msg.OrderingKey != "" {
		key = []byte(msg.OrderingKey)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: msg.Payload,
	}

	return t.cl.ProduceSync(ctx, record).FirstErr()
}

func (t *kafkaTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	topic := t.mainTopic(sub.Topology)
	group := sub.Topology.Name() + "-group"
	if sub.Group != "" {
		group = sub.Topology.Name() + "-" + sub.Group
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(), // Bug K1 mitigation
	}
	opts = append(opts, topologyFetchOptions(sub.Topology)...)

	cl, err := t.newConsumer(opts...)
	if err != nil {
		return err
	}
	defer func() {
		if sub.WaitForHandlers != nil {
			sub.WaitForHandlers()
		}
		cl.Close()
	}()

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || fetches.Err() != nil {
			if errors.Is(fetches.Err(), context.Canceled) {
				return ctx.Err()
			}
			return fetches.Err()
		}

		for _, record := range fetches.Records() {
			msg := transport.Message{
				Payload:     record.Value,
				OrderingKey: string(record.Key),
			}

			// Extract the normalized attempt from either a v2 or v3 envelope.
			var env eventbus.Envelope
			count := 1
			if err := json.Unmarshal(record.Value, &env); err == nil {
				count = env.Attempt
				if env.V == 2 {
					count++
				}
				if count < 1 {
					count = 1
				}
			}

			deliver(&kafkaDelivery{
				transport: t,
				topo:      sub.Topology,
				cl:        cl,
				record:    record,
				msg:       msg,
				count:     count,
			})
		}
	}
}

// delayConsumer handles all delay tiers for a specific topology
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

func (t *kafkaTransport) delayConsumer(ctx context.Context, topo transport.Topology, cl *kgo.Client) {
	defer cl.Close()

	// Pause tracking
	paused := make(map[string]map[int32]bool)
	var pm sync.Mutex

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || fetches.Err() != nil {
			return
		}

		// When records are returned, some might be delayed.
		for _, record := range fetches.Records() {
			var env eventbus.Envelope
			if err := json.Unmarshal(record.Value, &env); err != nil {
				// Corrupted, commit and skip
				cl.MarkCommitRecords(record)
				continue
			}

			runAt := time.UnixMilli(env.RunAt)
			delay := time.Until(runAt)

			topic := record.Topic
			part := record.Partition

			if delay > 0 {
				// Schedule execution
				// We keep the record in memory closure. franz-go bounds fetch sizes so it won't OOM.
				// If process crashes, it's not marked so it will be re-fetched on restart.
				time.AfterFunc(delay, func() {
					msg := transport.Message{
						Payload:     record.Value,
						OrderingKey: env.OrderingKey,
					}
					err := t.Publish(context.Background(), topo, msg)
					if err == nil {
						cl.MarkCommitRecords(record)
					} else {
						// If publish fails, we don't commit. On restart or next fetch it will be retried.
						// We could log this if we had a logger.
					}

					pm.Lock()
					if paused[topic][part] {
						cl.ResumeFetchPartitions(map[string][]int32{topic: {part}})
						paused[topic][part] = false
					}
					pm.Unlock()
				})

				// Pause this partition so we don't pull too many into memory
				pm.Lock()
				if paused[topic] == nil {
					paused[topic] = make(map[int32]bool)
				}
				if !paused[topic][part] {
					cl.PauseFetchPartitions(map[string][]int32{topic: {part}})
					paused[topic][part] = true
				}
				pm.Unlock()

			} else {
				// Ready to execute, forward to main topic
				msg := transport.Message{
					Payload:     record.Value,
					OrderingKey: env.OrderingKey,
				}
				err := t.Publish(context.Background(), topo, msg)
				if err == nil {
					cl.MarkCommitRecords(record)
				}
			}
		}
	}
}

func (t *kafkaTransport) Ping(ctx context.Context) error { return t.cl.Ping(ctx) }
func (t *kafkaTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	for _, cancel := range t.sweepers {
		cancel()
	}
	t.sweepers = make(map[string]context.CancelFunc)
	owned := t.producerOwned
	t.producerOwned = false
	cl := t.cl
	t.mu.Unlock()
	if owned && cl != nil {
		cl.Close()
	}
	return nil
}

// kafkaDelivery implements Delivery
type kafkaDelivery struct {
	transport *kafkaTransport
	topo      transport.Topology
	cl        *kgo.Client
	record    *kgo.Record
	msg       transport.Message
	count     int
	settled   transport.SettlementGuard
}

func (d *kafkaDelivery) Message() transport.Message { return d.msg }
func (d *kafkaDelivery) DeliveryAttempt() int       { return d.count }

func (d *kafkaDelivery) ack() error {
	d.cl.MarkCommitRecords(d.record)
	return nil
}

func (d *kafkaDelivery) deadLetter(ctx context.Context, decision transport.Decision) error {
	msg := d.msg
	if decision.Source != nil {
		msg = *decision.Source
	}
	dlq := d.transport.dlqTopic(d.topo)

	record := &kgo.Record{
		Topic: dlq,
		Value: msg.Payload,
		Key:   []byte(msg.OrderingKey),
	}
	for key, value := range msg.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	err := d.transport.cl.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return err
	}
	return d.ack()
}

func (d *kafkaDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		switch decision.Kind {
		case transport.DecisionAck:
			return d.ack()
		case transport.DecisionRetry:
			if err := d.transport.PublishAt(ctx, d.topo, decision.Next, decision.RetryAt); err != nil {
				return err
			}
			return d.ack()
		case transport.DecisionDeadLetter:
			return d.deadLetter(ctx, decision)
		default:
			return transport.ErrInvalidDecision
		}
	})
}

func (d *kafkaDelivery) Heartbeat(ctx context.Context) error {
	return nil
}
