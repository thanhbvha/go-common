package nats

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonnats "github.com/thanhbvha/go-common/nats"
)

func init() {
	registry.Register("nats", func() (transport.Transport, error) {
		c := commonnats.Default()
		if c == nil {
			return nil, fmt.Errorf("nats default client not set")
		}

		js := c.JetStream()
		if js == nil {
			return nil, fmt.Errorf("nats jetstream context is nil")
		}

		return New(js), nil
	})
}

type natsTransport struct {
	js gonats.JetStreamContext

	mu         sync.RWMutex
	kvs        map[string]gonats.KeyValue
	sweepers   map[string]context.CancelFunc
	sweeperWG  sync.WaitGroup
	instanceID string
	now        func() time.Time
	// afterTransfer is a fault-injection seam used by embedded broker tests at
	// the publish-before-source-settlement boundary.
	afterTransfer func(transport.DecisionKind) error
}

const dedupeBucket = "eventbus_dedupe_v2"

const messageIDHeader = "X-EventBus-Message-Id"

var (
	_ transport.Transport         = (*natsTransport)(nil)
	_ transport.Deduper           = (*natsTransport)(nil)
	_ transport.TopologyDeduper   = (*natsTransport)(nil)
	_ transport.DLQAdmin          = (*natsTransport)(nil)
	_ transport.CursorDLQAdmin    = (*natsTransport)(nil)
	_ transport.StatsReader       = (*natsTransport)(nil)
	_ transport.TopologyValidator = (*natsTransport)(nil)
	_ transport.Delivery          = (*natsDelivery)(nil)
	_ transport.LeaseDelivery     = (*natsDelivery)(nil)
)

func New(js gonats.JetStreamContext) transport.Transport {
	return &natsTransport{
		js:         js,
		kvs:        make(map[string]gonats.KeyValue),
		sweepers:   make(map[string]context.CancelFunc),
		instanceID: newSchedulerID(),
		now:        time.Now,
	}
}

func (t *natsTransport) Name() string { return "nats" }

func (t *natsTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		DelayPrecision: 1 * time.Second, // Sweeper checks every second
		Ordering:       false,           // Standard jetstream consumers without strictly constrained max-inflight
	}
}

func (t *natsTransport) streamName(topo transport.Topology) string {
	return "eventbus_" + topo.NameWithSeparator("_")
}

func (t *natsTransport) dlqStreamName(topo transport.Topology) string {
	return "eventbus_dlq_" + topo.NameWithSeparator("_")
}

func (t *natsTransport) kvBucketName(topo transport.Topology) string {
	return "eventbus_delay_" + topo.NameWithSeparator("_")
}

func (t *natsTransport) getKV(bucket string) (gonats.KeyValue, error) {
	t.mu.RLock()
	kv, ok := t.kvs[bucket]
	t.mu.RUnlock()
	if ok {
		return kv, nil
	}
	return t.js.KeyValue(bucket)
}

func (t *natsTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	stream := t.streamName(topo)
	dlq := t.dlqStreamName(topo)
	kvBucket := t.kvBucketName(topo)

	if err := t.reconcileStream(ctx, t.desiredStreamConfig(topo, stream)); err != nil {
		return fmt.Errorf("nats EnsureTopology main: %w", err)
	}
	if err := t.reconcileStream(ctx, t.desiredStreamConfig(topo, dlq)); err != nil {
		return fmt.Errorf("nats EnsureTopology DLQ: %w", err)
	}

	// Create Delay KV
	kv, err := t.js.CreateKeyValue(&gonats.KeyValueConfig{
		Bucket:  kvBucket,
		Storage: gonats.FileStorage,
	})
	if err != nil {
		if errors.Is(err, gonats.ErrStreamNameAlreadyInUse) {
			kv, err = t.js.KeyValue(kvBucket)
			if err != nil {
				return fmt.Errorf("nats EnsureTopology get KV: %w", err)
			}
		} else {
			return fmt.Errorf("nats EnsureTopology CreateKeyValue: %w", err)
		}
	}

	// Reservations carry their own expiry so callers can request different
	// TTLs. The bucket itself must therefore remain durable and non-expiring.
	_, err = t.js.CreateKeyValue(&gonats.KeyValueConfig{
		Bucket:  dedupeBucket,
		Storage: gonats.FileStorage,
		History: 1,
	})
	if err != nil && !errors.Is(err, gonats.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("nats EnsureTopology Dedupe KV: %w", err)
	}

	t.mu.Lock()
	t.kvs[kvBucket] = kv
	t.mu.Unlock()
	if err := t.ensureDelayCursor(ctx, kv); err != nil {
		return fmt.Errorf("nats EnsureTopology delay cursor: %w", err)
	}

	topoName := topo.Name()
	t.mu.Lock()
	if _, ok := t.sweepers[topoName]; !ok {
		sweepCtx, cancel := context.WithCancel(context.Background())
		t.sweepers[topoName] = cancel
		t.sweeperWG.Add(1)
		go func() {
			defer t.sweeperWG.Done()
			t.delaySweeper(sweepCtx, topo, kv)
		}()
	}
	t.mu.Unlock()

	return nil
}

func (t *natsTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	_, err := t.publish(ctx, t.streamName(topo), msg.ID, msg.ID, msg.Payload)
	return err
}

func (t *natsTransport) publish(ctx context.Context, subject, logicalID, brokerID string, payload []byte) (*gonats.PubAck, error) {
	msg := &gonats.Msg{Subject: subject, Data: payload, Header: gonats.Header{}}
	if logicalID != "" {
		msg.Header.Set(messageIDHeader, logicalID)
	}
	if brokerID != "" {
		msg.Header.Set(gonats.MsgIdHdr, brokerID)
	}
	return t.js.PublishMsg(msg, gonats.Context(ctx))
}

func (t *natsTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	delay := runAt.Sub(t.now())
	if delay <= 0 {
		return t.Publish(ctx, topo, msg)
	}

	kvBucket := t.kvBucketName(topo)
	kv, err := t.getKV(kvBucket)
	if err != nil {
		return err
	}

	return t.schedule(ctx, kv, msg, runAt)
}

func (t *natsTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	stream := t.streamName(sub.Topology)
	group := t.consumerName(sub)
	if err := t.reconcileConsumer(ctx, stream, sub); err != nil {
		return fmt.Errorf("nats provision consumer: %w", err)
	}

	opts := []gonats.SubOpt{
		gonats.ManualAck(),
		gonats.Bind(stream, group),
	}

	// Use durable Pull Consumer (Bind to the one we just created)
	nsub, err := t.js.PullSubscribe(stream, group, opts...)
	if err != nil {
		return err
	}
	// Bind attaches to an existing durable: Unsubscribe releases the local
	// subscription without deleting that durable consumer on the server.
	defer nsub.Unsubscribe()

	batchSize := 1
	if val, ok := sub.Topology.Options["nats.batch_size"].(int); ok && val > 0 {
		batchSize = val
	}

	fetchTimeout := 2 * time.Second
	if val, ok := sub.Topology.Options["nats.fetch_timeout"].(time.Duration); ok && val > 0 {
		fetchTimeout = val
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
			msgs, err := nsub.Fetch(batchSize, gonats.Context(fetchCtx))
			cancel()

			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					if ctx.Err() != nil {
						return ctx.Err() // Outer context canceled
					}
					continue // Just fetchCtx timeout, retry
				}
				if errors.Is(err, gonats.ErrTimeout) {
					continue // Fetch timeout, just retry
				}
				return err
			}

			for _, m := range msgs {
				// Deliver
				meta, err := m.Metadata()
				count := 1
				if err == nil {
					count = int(meta.NumDelivered)
				}

				id := m.Header.Get(messageIDHeader)
				if id == "" {
					id = m.Header.Get(gonats.MsgIdHdr)
				}
				msg := transport.Message{
					ID:      id,
					Payload: m.Data,
				}

				deliver(&natsDelivery{
					transport:         t,
					topo:              sub.Topology,
					msg:               msg,
					nmsg:              m,
					count:             count,
					heartbeatInterval: heartbeatInterval(topologyAckWait(sub.Topology)),
				})
			}
		}
	}
}

func (t *natsTransport) Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return t.reserve(ctx, dedupeStorageKey(nil, key), ttl)
}

func (t *natsTransport) Release(ctx context.Context, key string) error {
	return t.release(ctx, dedupeStorageKey(nil, key))
}

func (t *natsTransport) ReserveTopology(ctx context.Context, topo transport.Topology, key string, ttl time.Duration) (bool, error) {
	if err := topo.Validate(); err != nil {
		return false, err
	}
	return t.reserve(ctx, dedupeStorageKey(&topo, key), ttl)
}

func (t *natsTransport) ReleaseTopology(ctx context.Context, topo transport.Topology, key string) error {
	if err := topo.Validate(); err != nil {
		return err
	}
	return t.release(ctx, dedupeStorageKey(&topo, key))
}

func dedupeStorageKey(topo *transport.Topology, key string) string {
	encode := func(value string) string {
		return "x" + base64.RawURLEncoding.EncodeToString([]byte(value))
	}
	if topo == nil {
		return "v1.legacy." + encode(key)
	}
	return fmt.Sprintf("v1.%s.%d.%s.%s", encode(topo.Namespace), topo.Kind, encode(topo.JobType), encode(key))
}

func (t *natsTransport) reserve(ctx context.Context, storageKey string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("nats dedupe: TTL must be positive")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	kv, err := t.js.KeyValue(dedupeBucket)
	if err != nil {
		return false, fmt.Errorf("nats dedupe open bucket: %w", err)
	}
	expires := []byte(strconv.FormatInt(t.now().Add(ttl).UnixNano(), 10))
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if _, err := kv.Create(storageKey, expires); err == nil {
			return true, nil
		} else if !errors.Is(err, gonats.ErrKeyExists) {
			return false, fmt.Errorf("nats dedupe create: %w", err)
		}

		entry, err := kv.Get(storageKey)
		if errors.Is(err, gonats.ErrKeyNotFound) || errors.Is(err, gonats.ErrKeyDeleted) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("nats dedupe read: %w", err)
		}
		expiresAt, err := strconv.ParseInt(string(entry.Value()), 10, 64)
		if err != nil {
			return false, fmt.Errorf("nats dedupe invalid expiry for %q: %w", storageKey, err)
		}
		if t.now().UnixNano() < expiresAt {
			return false, nil
		}
		if _, err := kv.Update(storageKey, expires, entry.Revision()); err == nil {
			return true, nil
		} else if errors.Is(err, gonats.ErrKeyExists) {
			continue // another contender changed the revision; inspect it again
		} else {
			return false, fmt.Errorf("nats dedupe replace expired reservation: %w", err)
		}
	}
}

func (t *natsTransport) release(ctx context.Context, storageKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kv, err := t.js.KeyValue(dedupeBucket)
	if err != nil {
		return fmt.Errorf("nats dedupe open bucket: %w", err)
	}
	if err := kv.Delete(storageKey); err != nil && !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		return fmt.Errorf("nats dedupe release: %w", err)
	}
	return nil
}

func (t *natsTransport) Ping(ctx context.Context) error {
	// Ping not directly exposed on JS, we can fetch account info
	_, err := t.js.AccountInfo()
	return err
}

func (t *natsTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	for _, cancel := range t.sweepers {
		cancel()
	}
	t.sweepers = make(map[string]context.CancelFunc)
	t.mu.Unlock()
	done := make(chan struct{})
	go func() {
		t.sweeperWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// natsDelivery implements Delivery
type natsDelivery struct {
	transport         *natsTransport
	topo              transport.Topology
	msg               transport.Message
	nmsg              *gonats.Msg
	count             int
	heartbeatInterval time.Duration
	settled           transport.SettlementGuard
}

func (d *natsDelivery) Message() transport.Message { return d.msg }
func (d *natsDelivery) DeliveryAttempt() int       { return d.count }

func ackOptions(ctx context.Context) []gonats.AckOpt {
	if ctx.Done() != nil {
		return []gonats.AckOpt{gonats.Context(ctx)}
	}
	return []gonats.AckOpt{gonats.AckWait(5 * time.Second)}
}

func (d *natsDelivery) ack(ctx context.Context) error {
	return d.nmsg.AckSync(ackOptions(ctx)...)
}

func (d *natsDelivery) transferID(kind string) string {
	if metadata, err := d.nmsg.Metadata(); err == nil {
		return stableTransferID(kind, fmt.Sprintf("%s:%s:%d", metadata.Stream, metadata.Consumer, metadata.Sequence.Stream))
	}
	return stableTransferID(kind, d.msg.ID+":"+string(d.msg.Payload))
}

func (d *natsDelivery) transferred(kind transport.DecisionKind) error {
	if d.transport.afterTransfer != nil {
		return d.transport.afterTransfer(kind)
	}
	return nil
}

func (d *natsDelivery) deadLetter(ctx context.Context, decision transport.Decision) error {
	msg := d.msg
	if decision.Source != nil {
		msg = *decision.Source
	}
	record, err := d.deadLetterRecord(msg, decision)
	if err != nil {
		return err
	}
	dlq := d.transport.dlqStreamName(d.topo)

	brokerID := d.transferID("dlq")
	_, err = d.transport.publish(ctx, dlq, msg.ID, brokerID, record)
	if err != nil {
		return err
	}
	if err := d.transferred(transport.DecisionDeadLetter); err != nil {
		return err
	}
	// Context makes Term a request/reply operation, so settlement success is
	// recorded only after the server confirms the terminal acknowledgement.
	return d.nmsg.Term(ackOptions(ctx)...)
}

func (d *natsDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		switch decision.Kind {
		case transport.DecisionAck:
			return d.ack(ctx)
		case transport.DecisionRetry:
			if err := d.transport.scheduleRetry(ctx, d.topo, decision.Next, decision.RetryAt, d.transferID("retry")); err != nil {
				return err
			}
			if err := d.transferred(transport.DecisionRetry); err != nil {
				return err
			}
			return d.ack(ctx)
		case transport.DecisionDeadLetter:
			return d.deadLetter(ctx, decision)
		default:
			return transport.ErrInvalidDecision
		}
	})
}

func (d *natsDelivery) Heartbeat(ctx context.Context) error {
	return d.nmsg.InProgress(ackOptions(ctx)...)
}

func (d *natsDelivery) HeartbeatInterval() time.Duration { return d.heartbeatInterval }
