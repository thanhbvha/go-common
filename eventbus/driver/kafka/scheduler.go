package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultDelayBatchSize  = 100
	maxDelayBatchSize      = 1000
	delayPollTimeout       = time.Second
	delaySchedulerInterval = 5 * time.Second
)

type delaySchedulerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type delayConsumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	AllowRebalance()
	CloseAllowingRebalance()
}

func delayBatchSize(topo transport.Topology) int {
	if value, ok := topo.Options["kafka.delay_batch_size"].(int); ok {
		return value
	}
	return defaultDelayBatchSize
}

func (t *kafkaTransport) validateRelatedTopicPartitions(ctx context.Context, topics []string, expected int32) error {
	backoff := 25 * time.Millisecond
	for {
		ready, err := t.relatedTopicPartitionsReady(ctx, topics, expected)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("kafka: wait for related topic metadata: %w", ctx.Err())
		case <-timer.C:
		}
		backoff = min(250*time.Millisecond, backoff*2)
	}
}

func (t *kafkaTransport) relatedTopicPartitionsReady(ctx context.Context, topics []string, expected int32) (bool, error) {
	metadata, err := t.adm.Metadata(ctx, topics...)
	if err != nil {
		return false, fmt.Errorf("kafka: inspect related topics: %w", err)
	}
	for _, topic := range topics {
		detail, ok := metadata.Topics[topic]
		if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) || errors.Is(detail.Err, kerr.LeaderNotAvailable) {
			return false, nil
		}
		if detail.Err != nil {
			return false, fmt.Errorf("kafka: inspect related topic %q: %w", topic, detail.Err)
		}
		if got := int32(len(detail.Partitions)); got != expected {
			return false, fmt.Errorf("kafka: related topic %q has %d partitions, want %d", topic, got, expected)
		}
		for partition, partitionDetail := range detail.Partitions {
			if errors.Is(partitionDetail.Err, kerr.UnknownTopicOrPartition) || errors.Is(partitionDetail.Err, kerr.LeaderNotAvailable) {
				return false, nil
			}
			if partitionDetail.Err != nil {
				return false, fmt.Errorf("kafka: inspect related topic %q partition %d: %w", topic, partition, partitionDetail.Err)
			}
			if partitionDetail.Leader < 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func (t *kafkaTransport) startDelayScheduler(topo transport.Topology) (*delaySchedulerHandle, error) {
	type tierConsumer struct {
		tier     time.Duration
		targeted bool
		client   *kgo.Client
	}
	consumers := make([]tierConsumer, 0, len(delayTiers)*2)
	for _, tier := range delayTiers {
		workers := []tierConsumer{{tier: tier}}
		if topo.Kind == transport.EventTopology {
			workers = append(workers, tierConsumer{tier: tier, targeted: true})
		}
		for _, worker := range workers {
			topic, group := t.delayTopic(topo, tier), fmt.Sprintf("%s-delay-%s-group", topo.Name(), tier)
			if worker.targeted {
				topic, group = t.targetedDelayTopic(topo, tier), fmt.Sprintf("%s-target-delay-%s-group", topo.Name(), tier)
			}
			opts := []kgo.Opt{
				kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(),
			}
			opts = append(opts, topologyFetchOptions(topo)...)
			client, err := t.newManualConsumer(opts...)
			if err != nil {
				for _, consumer := range consumers {
					consumer.client.CloseAllowingRebalance()
				}
				return nil, err
			}
			worker.client = client
			consumers = append(consumers, worker)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle := &delaySchedulerHandle{cancel: cancel, done: make(chan struct{})}
	var workers sync.WaitGroup
	workers.Add(len(consumers))
	for _, consumer := range consumers {
		go func(tier time.Duration, targeted bool, client *kgo.Client) {
			defer workers.Done()
			t.runDelayTier(ctx, topo, tier, targeted, client)
		}(consumer.tier, consumer.targeted, consumer.client)
	}
	go func() {
		workers.Wait()
		close(handle.done)
	}()
	return handle, nil
}

func (t *kafkaTransport) runDelayTier(ctx context.Context, topo transport.Topology, tier time.Duration, targeted bool, client delayConsumerClient) {
	defer client.CloseAllowingRebalance()
	ticker := time.NewTicker(delaySchedulerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.recordSchedulerPoll(topo, schedulerWorker(tier, targeted), time.Now())
			pollCtx, cancel := context.WithTimeout(ctx, delayPollTimeout)
			fetches := client.PollRecords(pollCtx, delayBatchSize(topo))
			cancel()
			if fetches.IsClientClosed() || ctx.Err() != nil {
				return
			}
			if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.recordSchedulerError(topo, schedulerWorker(tier, targeted), "poll", err, time.Now())
				client.AllowRebalance()
				continue
			}
			if len(fetches.Records()) > 0 {
				_ = t.processDelayBatchFor(ctx, topo, tier, targeted, client, fetches.Records(), time.Now())
			} else {
				t.recordSchedulerSuccess(topo, schedulerWorker(tier, targeted), 0, time.Now())
			}
			client.AllowRebalance()
		}
	}
}

// processDelayBatch performs a bounded sweep. Records that still belong in
// their current tier are rewound and stay durable in Kafka. A record moves to a
// lower tier or the main topic only after confirmed production, and its source
// offset is committed synchronously afterward.
func (t *kafkaTransport) processDelayBatch(ctx context.Context, topo transport.Topology, tier time.Duration, client delayConsumerClient, records []*kgo.Record, now time.Time) error {
	return t.processDelayBatchFor(ctx, topo, tier, false, client, records, now)
}

func (t *kafkaTransport) processDelayBatchFor(ctx context.Context, topo transport.Topology, tier time.Duration, targeted bool, client delayConsumerClient, records []*kgo.Record, now time.Time) error {
	queues := make(map[partitionKey][]*kgo.Record)
	keys := make([]partitionKey, 0)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		if _, exists := queues[key]; !exists {
			keys = append(keys, key)
		}
		queues[key] = append(queues[key], record)
	}
	rewind := make(map[string]map[int32]kgo.EpochOffset)
	var failures []error
	transfers, commits := 0, 0
	for _, key := range keys {
		for _, record := range queues[key] {
			expectedTopic := t.delayTopic(topo, tier)
			if targeted {
				expectedTopic = t.targetedDelayTopic(topo, tier)
			}
			if record.Topic != expectedTopic {
				setDelayRewind(rewind, record)
				failures = append(failures, fmt.Errorf("kafka: delay tier %s received unexpected topic %q", tier, record.Topic))
				break
			}
			runAt, err := delayedRecordRunAt(record)
			if err != nil {
				setDelayRewind(rewind, record)
				failures = append(failures, fmt.Errorf("kafka: decode delayed record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err))
				break
			}
			message := recordMessage(record)
			target := t.delayDestination(topo, runAt, now)
			if targeted {
				group := message.Headers[kafkaTargetGroupHeader]
				if group == "" {
					setDelayRewind(rewind, record)
					failures = append(failures, fmt.Errorf("kafka: targeted delayed record %s[%d]@%d has no target group", record.Topic, record.Partition, record.Offset))
					break
				}
				if err := t.validateProvisionedSubscriber(ctx, topo, group); err != nil {
					setDelayRewind(rewind, record)
					failures = append(failures, fmt.Errorf("kafka: targeted delayed record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err))
					break
				}
				target = t.targetedDelayDestination(topo, group, runAt, now)
				if target == t.privateReadyTopic(topo, group) {
					removeMessageHeader(&message, kafkaTargetGroupHeader)
				}
			}
			// Rotate a not-yet-due partition head to the tail of the same durable
			// tier. Confirmed publish-before-commit lets later, earlier deadlines
			// progress without losing the blocker on an ambiguous boundary.
			if target == t.mainTopic(topo) || targeted && !strings.Contains(target, ".target-delay-") {
				removeMessageHeader(&message, kafkaRunAtHeader)
			}
			if err := t.produceConfirmed(ctx, messageRecord(target, message)); err != nil {
				setDelayRewind(rewind, record)
				failures = append(failures, fmt.Errorf("kafka: forward delayed record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err))
				break
			}
			transfers++
			if err := client.CommitRecords(ctx, record); err != nil {
				setDelayRewind(rewind, record)
				failures = append(failures, fmt.Errorf("kafka: commit delayed record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err))
				break
			}
			commits++
		}
	}
	if len(rewind) > 0 {
		client.SetOffsets(rewind)
	}
	err := errors.Join(failures...)
	t.recordSchedulerProgress(topo, schedulerWorker(tier, targeted), transfers, commits)
	if err == nil {
		t.recordSchedulerSuccess(topo, schedulerWorker(tier, targeted), len(records), now)
	} else {
		t.recordSchedulerError(topo, schedulerWorker(tier, targeted), kafkaSchedulerOperation(err), err, now)
	}
	return err
}

func (t *kafkaTransport) recordSchedulerProgress(topo transport.Topology, worker string, transfers, commits int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	status := t.schedulerStatusLocked(topo, worker)
	status.Transfers += uint64(transfers)
	status.Commits += uint64(commits)
}

func schedulerWorker(tier time.Duration, targeted bool) string {
	if targeted {
		return "target-" + tier.String()
	}
	return tier.String()
}

func (t *kafkaTransport) recordSchedulerPoll(topo transport.Topology, worker string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	status := t.schedulerStatusLocked(topo, worker)
	status.LastPoll = now
	status.Polls++
}

func (t *kafkaTransport) recordSchedulerSuccess(topo transport.Topology, worker string, processed int, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	status := t.schedulerStatusLocked(topo, worker)
	status.LastSuccess = now
	status.Dispatches++
	status.ConsecutiveErrors = 0
	status.Degraded = false
}

func (t *kafkaTransport) recordSchedulerError(topo transport.Topology, worker, operation string, err error, now time.Time) {
	if errors.Is(err, context.Canceled) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	status := t.schedulerStatusLocked(topo, worker)
	status.LastErrorAt = now
	status.LastErrorOperation = operation
	status.LastError = kafkaSchedulerError(err)
	status.Retries++
	status.ConsecutiveErrors++
	status.Degraded = true
}

func (t *kafkaTransport) schedulerStatusLocked(topo transport.Topology, worker string) *transport.SchedulerStatus {
	key := topo.Name()
	if t.schedulerStatuses == nil {
		t.schedulerStatuses = make(map[string]map[string]*transport.SchedulerStatus)
	}
	if t.schedulerStatuses[key] == nil {
		t.schedulerStatuses[key] = make(map[string]*transport.SchedulerStatus)
	}
	status := t.schedulerStatuses[key][worker]
	if status == nil {
		status = &transport.SchedulerStatus{Transport: "kafka", Topology: key, Worker: worker}
		t.schedulerStatuses[key][worker] = status
	}
	return status
}

func (t *kafkaTransport) SchedulerHealth(topo transport.Topology) []transport.SchedulerStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	workers := t.schedulerStatuses[topo.Name()]
	result := make([]transport.SchedulerStatus, 0, len(delayTiers))
	for _, tier := range delayTiers {
		if status := workers[tier.String()]; status != nil {
			result = append(result, *status)
		}
		if status := workers[schedulerWorker(tier, true)]; status != nil {
			result = append(result, *status)
		}
	}
	return result
}

func kafkaSchedulerError(err error) string {
	if err == nil {
		return ""
	}
	text := fmt.Sprintf("%T", err)
	if len(text) > transport.MaxSchedulerErrorBytes {
		text = text[:transport.MaxSchedulerErrorBytes]
	}
	return text
}

func kafkaSchedulerOperation(err error) string {
	text := err.Error()
	if strings.Contains(text, "forward delayed record") {
		return "transfer"
	}
	if strings.Contains(text, "commit delayed record") {
		return "commit"
	}
	return "dispatch"
}

func delayedRecordRunAt(record *kgo.Record) (time.Time, error) {
	for index := len(record.Headers) - 1; index >= 0; index-- {
		if record.Headers[index].Key != kafkaRunAtHeader {
			continue
		}
		millis, err := strconv.ParseInt(string(record.Headers[index].Value), 10, 64)
		if err != nil || millis <= 0 {
			return time.Time{}, fmt.Errorf("invalid %s header", kafkaRunAtHeader)
		}
		return time.UnixMilli(millis), nil
	}
	var envelope eventbus.Envelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil || envelope.RunAt <= 0 {
		return time.Time{}, fmt.Errorf("missing schedule metadata")
	}
	return time.UnixMilli(envelope.RunAt), nil
}

func (t *kafkaTransport) delayDestination(topo transport.Topology, runAt, now time.Time) string {
	delay := runAt.Sub(now)
	if delay <= 0 {
		return t.mainTopic(topo)
	}
	tier := delayTiers[0]
	for i := len(delayTiers) - 1; i >= 0; i-- {
		if delay >= delayTiers[i] {
			tier = delayTiers[i]
			break
		}
	}
	return t.delayTopic(topo, tier)
}

func (t *kafkaTransport) targetedDelayDestination(topo transport.Topology, group string, runAt, now time.Time) string {
	delay := runAt.Sub(now)
	if delay <= 0 {
		return t.privateReadyTopic(topo, group)
	}
	tier := delayTiers[0]
	for i := len(delayTiers) - 1; i >= 0; i-- {
		if delay >= delayTiers[i] {
			tier = delayTiers[i]
			break
		}
	}
	return t.targetedDelayTopic(topo, tier)
}

func setDelayRewind(offsets map[string]map[int32]kgo.EpochOffset, record *kgo.Record) {
	partitions := offsets[record.Topic]
	if partitions == nil {
		partitions = make(map[int32]kgo.EpochOffset)
		offsets[record.Topic] = partitions
	}
	if existing, ok := partitions[record.Partition]; !ok || record.Offset < existing.Offset {
		partitions[record.Partition] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
	}
}
