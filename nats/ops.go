package nats

import (
	"context"
	"fmt"
	"time"

	gonats "github.com/nats-io/nats.go"
)

// ============================================================
// Section 1 – Core Pub/Sub
// ============================================================

// Publish sends data to subject via NATS Core (at-most-once delivery).
func (c *Client) Publish(subject string, data []byte) error {
	nc, _, err := c.requireConnected()
	if err != nil {
		return err
	}
	return nc.Publish(subject, data)
}

// PublishMsg sends a pre-built *nats.Msg via NATS Core.
func (c *Client) PublishMsg(msg *gonats.Msg) error {
	nc, _, err := c.requireConnected()
	if err != nil {
		return err
	}
	return nc.PublishMsg(msg)
}

// Subscribe registers handler for every message arriving on subject.
// Returns the subscription handle; call Unsubscribe when done.
func (c *Client) Subscribe(subject string, handler gonats.MsgHandler) (*gonats.Subscription, error) {
	nc, _, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return nc.Subscribe(subject, handler)
}

// QueueSubscribe registers handler as part of a queue group.
// Only one member of the group receives each message (load-balanced).
func (c *Client) QueueSubscribe(subject, queue string, handler gonats.MsgHandler) (*gonats.Subscription, error) {
	nc, _, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return nc.QueueSubscribe(subject, queue, handler)
}

// Request sends data to subject and waits for a single reply.
func (c *Client) Request(ctx context.Context, subject string, data []byte) (*gonats.Msg, error) {
	nc, _, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	timeout := 5 * time.Second
	if ok {
		timeout = time.Until(deadline)
	}
	return nc.Request(subject, data, timeout)
}

// Unsubscribe drains and removes the subscription.
func (c *Client) Unsubscribe(sub *gonats.Subscription) error {
	if sub == nil {
		return nil
	}
	return sub.Unsubscribe()
}

// ============================================================
// Section 2 – JetStream Stream Management  (≈ XGroupCreateMkStream / XInfoGroups)
// ============================================================

// StreamInfo holds a simplified view of a JetStream stream's state.
type StreamInfo struct {
	Name       string
	Subjects   []string
	Msgs       uint64
	Bytes      uint64
	FirstSeq   uint64
	LastSeq    uint64
	NumDeleted uint64
	Storage    StorageType
	Replicas   int
}

// AddStream creates a new stream. If the stream already exists with identical
// configuration the call is a no-op; differing configuration returns an error.
func (c *Client) AddStream(ctx context.Context, name string, subjects []string, opts ...StreamOption) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}

	storage := gonats.MemoryStorage
	if c.cfg.DefaultStorage == FileStorage {
		storage = gonats.FileStorage
	}

	cfg := &gonats.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  storage,
		Replicas: c.cfg.DefaultReplicas,
	}
	for _, o := range opts {
		o(cfg)
	}

	_, err = js.AddStream(cfg)
	return err
}

// UpdateStream updates an existing stream's configuration.
func (c *Client) UpdateStream(ctx context.Context, name string, opts ...StreamOption) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	info, err := js.StreamInfo(name)
	if err != nil {
		return fmt.Errorf("nats: stream %q not found: %w", name, err)
	}
	cfg := &info.Config
	for _, o := range opts {
		o(cfg)
	}
	_, err = js.UpdateStream(cfg)
	return err
}

// DeleteStream permanently removes a stream and all its messages.
func (c *Client) DeleteStream(ctx context.Context, name string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	return js.DeleteStream(name)
}

// GetStreamInfo returns metadata and current state of a stream.
func (c *Client) GetStreamInfo(ctx context.Context, name string) (*StreamInfo, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	info, err := js.StreamInfo(name)
	if err != nil {
		return nil, err
	}
	return toStreamInfo(info), nil
}

// ListStreams returns metadata for all streams visible to this connection.
func (c *Client) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	var result []StreamInfo
	for info := range js.StreamsInfo() {
		result = append(result, *toStreamInfo(info))
	}
	return result, nil
}

// PurgeStream removes all messages from a stream without deleting the stream itself.
// Equivalent to issuing XDel for every message ID.
func (c *Client) PurgeStream(ctx context.Context, name string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	return js.PurgeStream(name)
}

// StreamExists returns true when a stream with the given name exists.
func (c *Client) StreamExists(ctx context.Context, name string) (bool, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	_, err = js.StreamInfo(name)
	if err == gonats.ErrStreamNotFound {
		return false, nil
	}
	return err == nil, err
}

// ============================================================
// Section 3 – JetStream Publishing  (≈ XAdd)
// ============================================================

// PubAck is returned after a successful JSPublish, confirming persistence.
type PubAck struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

// JSPublish appends data to the stream bound to subject (at-least-once delivery).
// Blocks until the server acknowledges persistence. Equivalent to XADD.
func (c *Client) JSPublish(ctx context.Context, subject string, data []byte) (*PubAck, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	ack, err := js.Publish(subject, data, gonats.Context(ctx))
	if err != nil {
		return nil, err
	}
	return &PubAck{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// JSPublishMsg appends a pre-built message to JetStream.
func (c *Client) JSPublishMsg(ctx context.Context, msg *gonats.Msg) (*PubAck, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	ack, err := js.PublishMsg(msg, gonats.Context(ctx))
	if err != nil {
		return nil, err
	}
	return &PubAck{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// JSPublishAsync appends data to JetStream without blocking for the server ACK.
// Use js.PublishAsyncComplete() or the returned future to await acknowledgement.
func (c *Client) JSPublishAsync(subject string, data []byte) (gonats.PubAckFuture, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return js.PublishAsync(subject, data)
}

// ============================================================
// Section 4 – JetStream Consumer Management  (≈ XGroupCreate / XInfoConsumers)
// ============================================================

// ConsumerInfo holds a simplified view of a JetStream consumer's state.
type ConsumerInfo struct {
	Name           string
	Durable        string
	NumPending     uint64 // messages waiting to be delivered (≈ XPending total)
	NumAckPending  int    // delivered but not yet ACK-ed (≈ XPending in-flight)
	NumRedelivered uint64
	AckWait        time.Duration
}

// AddConsumer creates or updates a durable consumer on stream.
func (c *Client) AddConsumer(ctx context.Context, stream, durable string, opts ...ConsumerOption) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	cfg := &gonats.ConsumerConfig{
		Durable:   durable,
		AckPolicy: gonats.AckExplicitPolicy,
		AckWait:   30 * time.Second,
	}
	for _, o := range opts {
		o(cfg)
	}
	_, err = js.AddConsumer(stream, cfg)
	return err
}

// DeleteConsumer removes a consumer from stream.
func (c *Client) DeleteConsumer(ctx context.Context, stream, consumer string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	return js.DeleteConsumer(stream, consumer)
}

// GetConsumerInfo returns state for a specific consumer.
func (c *Client) GetConsumerInfo(ctx context.Context, stream, consumer string) (*ConsumerInfo, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	info, err := js.ConsumerInfo(stream, consumer)
	if err != nil {
		return nil, err
	}
	return toConsumerInfo(info), nil
}

// ListConsumers returns state for all consumers on stream.
// Equivalent to XINFO GROUPS / XINFO CONSUMERS.
func (c *Client) ListConsumers(ctx context.Context, stream string) ([]ConsumerInfo, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	var result []ConsumerInfo
	for info := range js.ConsumersInfo(stream) {
		result = append(result, *toConsumerInfo(info))
	}
	return result, nil
}

// ConsumerExists returns true when the named consumer exists on stream.
func (c *Client) ConsumerExists(ctx context.Context, stream, consumer string) (bool, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	_, err = js.ConsumerInfo(stream, consumer)
	if err == gonats.ErrConsumerNotFound {
		return false, nil
	}
	return err == nil, err
}

// ============================================================
// Section 5 – JetStream Push Subscribe  (≈ XReadGroup push mode)
// ============================================================

// MsgHandler is the callback signature for push-based JetStream subscriptions.
type MsgHandler func(*Msg)

// JSSubscribe creates a push-based subscription for a durable consumer.
// handler is called in a goroutine for every delivered message.
// Call WithManualAck() when you want to ACK/Nak explicitly.
func (c *Client) JSSubscribe(subject, durable string, handler MsgHandler, opts ...SubOption) (*gonats.Subscription, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}

	so := &subOptions{}
	for _, o := range opts {
		o(so)
	}

	jsOpts := []gonats.SubOpt{gonats.Durable(durable)}
	if so.manualAck {
		jsOpts = append(jsOpts, gonats.ManualAck())
	}
	if so.maxAckPending > 0 {
		jsOpts = append(jsOpts, gonats.MaxAckPending(so.maxAckPending))
	}

	return js.Subscribe(subject, func(m *gonats.Msg) {
		handler(wrapMsg(m))
	}, jsOpts...)
}

// JSQueueSubscribe creates a push-based queue subscription (competing consumers).
func (c *Client) JSQueueSubscribe(subject, queue, durable string, handler MsgHandler, opts ...SubOption) (*gonats.Subscription, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}

	so := &subOptions{}
	for _, o := range opts {
		o(so)
	}

	jsOpts := []gonats.SubOpt{gonats.Durable(durable)}
	if so.manualAck {
		jsOpts = append(jsOpts, gonats.ManualAck())
	}

	return js.QueueSubscribe(subject, queue, func(m *gonats.Msg) {
		handler(wrapMsg(m))
	}, jsOpts...)
}

// ============================================================
// Section 6 – JetStream Pull Subscribe  (≈ XReadGroup pull / blocking)
// ============================================================

// PullSubscription wraps a NATS pull subscription.
type PullSubscription struct {
	sub *gonats.Subscription
}

// PullSubscribe creates a pull-based subscription backed by a durable consumer.
// Callers explicitly call Fetch to retrieve batches of messages.
func (c *Client) PullSubscribe(subject, durable string, opts ...SubOption) (*PullSubscription, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	sub, err := js.PullSubscribe(subject, durable)
	if err != nil {
		return nil, err
	}
	return &PullSubscription{sub: sub}, nil
}

// Fetch retrieves up to count messages, blocking until at least one arrives
// or the fetch timeout elapses. Default timeout: 5s.
// Equivalent to XREADGROUP with COUNT and BLOCK.
func (c *Client) Fetch(ps *PullSubscription, count int, opts ...FetchOption) ([]*Msg, error) {
	fo := &fetchOptions{timeout: 5 * time.Second}
	for _, o := range opts {
		o(fo)
	}
	raw, err := ps.sub.Fetch(count, gonats.MaxWait(fo.timeout))
	if err != nil {
		return nil, err
	}
	msgs := make([]*Msg, len(raw))
	for i, m := range raw {
		msgs[i] = wrapMsg(m)
	}
	return msgs, nil
}

// FetchNoWait retrieves up to count messages without blocking.
// Returns immediately with whatever is available (may be empty).
func (c *Client) FetchNoWait(ps *PullSubscription, count int) ([]*Msg, error) {
	raw, err := ps.sub.Fetch(count, gonats.MaxWait(time.Millisecond))
	if err == gonats.ErrTimeout {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	msgs := make([]*Msg, len(raw))
	for i, m := range raw {
		msgs[i] = wrapMsg(m)
	}
	return msgs, nil
}

// Unsubscribe cancels a pull subscription.
func (ps *PullSubscription) Unsubscribe() error {
	return ps.sub.Unsubscribe()
}

// ============================================================
// Section 7 – Msg & ACK  (≈ XAck / XClaim / NakWithDelay)
// ============================================================

// Msg is a wrapper around *nats.Msg that exposes JetStream metadata.
type Msg struct {
	Subject      string
	Reply        string
	Data         []byte
	Headers      gonats.Header
	Sequence     uint64        // ≈ Redis Stream message ID
	Stream       string
	Consumer     string
	NumDelivered uint64        // ≈ XPendingExt DeliveryCount
	NumPending   uint64
	Redelivered  bool
	raw          *gonats.Msg
}

// Ack acknowledges the message, removing it from the pending list.
// Equivalent to XACK.
func (m *Msg) Ack() error { return m.raw.Ack() }

// Nak signals that processing failed; the server redelivers immediately.
func (m *Msg) Nak() error { return m.raw.Nak() }

// NakWithDelay signals failure and requests redelivery after delay.
// Equivalent to using XCLAIM to defer reprocessing.
func (m *Msg) NakWithDelay(delay time.Duration) error { return m.raw.NakWithDelay(delay) }

// Term instructs the server to never redeliver this message (dead-lettered).
// Use when MaxDeliver is reached or the message is fundamentally unprocessable.
func (m *Msg) Term() error { return m.raw.Term() }

// InProgress resets the AckWait timer, signalling that processing is ongoing.
// Call periodically for long-running tasks to prevent premature redelivery.
func (m *Msg) InProgress() error { return m.raw.InProgress() }

// Raw returns the underlying *nats.Msg for advanced use.
func (m *Msg) Raw() *gonats.Msg { return m.raw }

// ============================================================
// Section 8 – KV Store  (≈ Redis Hash/String + Watch + History)
// ============================================================

// KVEntry is a single key-value record returned from the KV store.
type KVEntry struct {
	Bucket    string
	Key       string
	Value     []byte
	Revision  uint64            // use for optimistic locking in KVUpdate
	Operation gonats.KeyValueOp // KeyValuePut / KeyValueDelete / KeyValuePurge
}

// KVCreate creates a new KV bucket with the given options.
// If the bucket already exists this is a no-op (idempotent).
func (c *Client) KVCreate(ctx context.Context, bucket string, opts ...KVOption) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	storage := gonats.MemoryStorage
	if c.cfg.DefaultStorage == FileStorage {
		storage = gonats.FileStorage
	}
	cfg := &gonats.KeyValueConfig{
		Bucket:   bucket,
		Storage:  storage,
		Replicas: c.cfg.DefaultReplicas,
		History:  1,
	}
	for _, o := range opts {
		o(cfg)
	}
	_, err = js.CreateKeyValue(cfg)
	return err
}

// KVDeleteBucket removes the KV bucket and all its data permanently.
func (c *Client) KVDeleteBucket(ctx context.Context, bucket string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	return js.DeleteKeyValue(bucket)
}

// kvBucket is an internal helper that returns the KeyValue handle.
func (c *Client) kvBucket(js gonats.JetStreamContext, bucket string) (gonats.KeyValue, error) {
	kv, err := js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("nats: KV bucket %q not found: %w", bucket, err)
	}
	return kv, nil
}

// KVPut sets key to value and returns the new revision number.
func (c *Client) KVPut(ctx context.Context, bucket, key string, value []byte) (uint64, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return 0, err
	}
	return kv.Put(key, value)
}

// KVGet retrieves the current value for key.
func (c *Client) KVGet(ctx context.Context, bucket, key string) (*KVEntry, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(key)
	if err != nil {
		return nil, err
	}
	return toKVEntry(entry), nil
}

// KVGetRevision retrieves a specific historical revision of key.
func (c *Client) KVGetRevision(ctx context.Context, bucket, key string, revision uint64) (*KVEntry, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	entry, err := kv.GetRevision(key, revision)
	if err != nil {
		return nil, err
	}
	return toKVEntry(entry), nil
}

// KVUpdate sets key to value only if the current revision matches lastRevision.
// Returns the new revision on success, or an error if a concurrent write occurred
// (optimistic locking).
func (c *Client) KVUpdate(ctx context.Context, bucket, key string, value []byte, lastRevision uint64) (uint64, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return 0, err
	}
	return kv.Update(key, value, lastRevision)
}

// KVCreate2 sets key to value only if the key does not yet exist.
// Equivalent to Redis SET NX.
func (c *Client) KVCreate2(ctx context.Context, bucket, key string, value []byte) (uint64, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return 0, err
	}
	return kv.Create(key, value)
}

// KVDeleteKey soft-deletes a key by placing a delete marker (history preserved).
func (c *Client) KVDeleteKey(ctx context.Context, bucket, key string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return err
	}
	return kv.Delete(key)
}

// KVPurgeKey removes a key and all its history permanently.
func (c *Client) KVPurgeKey(ctx context.Context, bucket, key string) error {
	_, js, err := c.requireConnected()
	if err != nil {
		return err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return err
	}
	return kv.Purge(key)
}

// KVKeys returns all active keys in the bucket.
func (c *Client) KVKeys(ctx context.Context, bucket string) ([]string, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	return kv.Keys()
}

// KVWatch watches a single key and sends updates to the returned watcher.
// Call watcher.Stop() when done.
func (c *Client) KVWatch(ctx context.Context, bucket, key string) (gonats.KeyWatcher, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	return kv.Watch(key)
}

// KVWatchAll watches all keys in the bucket.
func (c *Client) KVWatchAll(ctx context.Context, bucket string) (gonats.KeyWatcher, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	return kv.WatchAll()
}

// KVHistory returns all stored revisions for a key (requires bucket History > 1).
func (c *Client) KVHistory(ctx context.Context, bucket, key string) ([]KVEntry, error) {
	_, js, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	kv, err := c.kvBucket(js, bucket)
	if err != nil {
		return nil, err
	}
	entries, err := kv.History(key)
	if err != nil {
		return nil, err
	}
	result := make([]KVEntry, len(entries))
	for i, e := range entries {
		result[i] = *toKVEntry(e)
	}
	return result, nil
}

// ============================================================
// Internal conversion helpers
// ============================================================

func wrapMsg(m *gonats.Msg) *Msg {
	meta, _ := m.Metadata()
	msg := &Msg{
		Subject: m.Subject,
		Reply:   m.Reply,
		Data:    m.Data,
		Headers: m.Header,
		raw:     m,
	}
	if meta != nil {
		msg.Sequence = meta.Sequence.Stream
		msg.Stream = meta.Stream
		msg.Consumer = meta.Consumer
		msg.NumDelivered = meta.NumDelivered
		msg.NumPending = meta.NumPending
		msg.Redelivered = meta.NumDelivered > 1
	}
	return msg
}

func toStreamInfo(info *gonats.StreamInfo) *StreamInfo {
	st := MemoryStorage
	if info.Config.Storage == gonats.FileStorage {
		st = FileStorage
	}
	return &StreamInfo{
		Name:       info.Config.Name,
		Subjects:   info.Config.Subjects,
		Msgs:       info.State.Msgs,
		Bytes:      info.State.Bytes,
		FirstSeq:   info.State.FirstSeq,
		LastSeq:    info.State.LastSeq,
		NumDeleted: uint64(info.State.NumDeleted),
		Storage:    st,
		Replicas:   info.Config.Replicas,
	}
}

func toConsumerInfo(info *gonats.ConsumerInfo) *ConsumerInfo {
	return &ConsumerInfo{
		Name:           info.Name,
		Durable:        info.Config.Durable,
		NumPending:     info.NumPending,
		NumAckPending:  info.NumAckPending,
		NumRedelivered: uint64(info.NumRedelivered),
		AckWait:        info.Config.AckWait,
	}
}

func toKVEntry(e gonats.KeyValueEntry) *KVEntry {
	return &KVEntry{
		Bucket:    e.Bucket(),
		Key:       e.Key(),
		Value:     e.Value(),
		Revision:  e.Revision(),
		Operation: e.Operation(),
	}
}
