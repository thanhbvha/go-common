package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	kafkaDLQRecordVersion = 1
	maxDLQAdminBatch      = 1000
)

type kafkaAdmin interface {
	CreateTopics(context.Context, int32, int16, map[string]*string, ...string) (kadm.CreateTopicResponses, error)
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
	AlterTopicConfigs(context.Context, []kadm.AlterConfig, ...string) (kadm.AlterConfigsResponses, error)
	CreatePartitions(context.Context, int, ...string) (kadm.CreatePartitionsResponses, error)
	DeleteTopics(context.Context, ...string) (kadm.DeleteTopicResponses, error)
	Metadata(context.Context, ...string) (kadm.Metadata, error)
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListGroups(context.Context, ...string) (kadm.ListedGroups, error)
	FetchOffsetsForTopics(context.Context, string, ...string) (kadm.OffsetResponses, error)
	CommitOffsets(context.Context, string, kadm.Offsets) (kadm.OffsetResponses, error)
}

type dlqPartitionScanner func(context.Context, string, int32, int64, int64, int) ([]*kgo.Record, error)

type kafkaDLQRecord struct {
	Version         int               `json:"v"`
	Message         transport.Message `json:"message"`
	Reason          string            `json:"reason"`
	LastError       string            `json:"error"`
	Attempts        int               `json:"attempts"`
	FailedAt        int64             `json:"failed_at"`
	SourceTopic     string            `json:"source_topic"`
	SourceGroup     string            `json:"source_group,omitempty"`
	SourcePartition int32             `json:"source_partition"`
	SourceOffset    int64             `json:"source_offset"`
	SourceTimestamp int64             `json:"source_timestamp"`
}

func (d *kafkaDelivery) deadLetterRecord(msg transport.Message, decision transport.Decision) ([]byte, error) {
	if len(msg.Payload) == 0 {
		return nil, fmt.Errorf("kafka: DLQ payload required")
	}
	failedAt := decision.FailedAt
	if failedAt.IsZero() {
		failedAt = time.Now()
	}
	record := kafkaDLQRecord{
		Version: kafkaDLQRecordVersion, Message: msg,
		Attempts: decision.Attempts, FailedAt: failedAt.UnixMilli(),
	}
	record.Reason, record.LastError = transport.NormalizeDecisionMetadata(decision)
	if d.record != nil {
		record.SourceTopic = d.record.Topic
		record.SourcePartition = d.record.Partition
		record.SourceOffset = d.record.Offset
		sourceTimestamp := d.record.Timestamp
		if sourceTimestamp.IsZero() {
			sourceTimestamp = failedAt
		}
		record.SourceTimestamp = sourceTimestamp.UnixMilli()
	}
	record.SourceGroup = d.group
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("kafka: encode DLQ record: %w", err)
	}
	if len(encoded) > transport.MaxEncodedDLQRecordBytes {
		return nil, fmt.Errorf("kafka: encoded DLQ record exceeds %d bytes", transport.MaxEncodedDLQRecordBytes)
	}
	return encoded, nil
}

func (t *kafkaTransport) dlqAdminGroup(topology transport.Topology) string {
	return topology.Name() + "-dlq-admin"
}

func kafkaDLQID(partition int32, offset int64) string {
	return strconv.FormatInt(int64(partition), 10) + ":" + strconv.FormatInt(offset, 10)
}

func parseKafkaDLQID(id string) (int32, int64, error) {
	partitionText, offsetText, ok := strings.Cut(id, ":")
	if !ok {
		return 0, 0, fmt.Errorf("kafka: invalid DLQ record ID")
	}
	partition, err := strconv.ParseInt(partitionText, 10, 32)
	if err != nil || partition < 0 {
		return 0, 0, fmt.Errorf("kafka: invalid DLQ record ID")
	}
	offset, err := strconv.ParseInt(offsetText, 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, fmt.Errorf("kafka: invalid DLQ record ID")
	}
	return int32(partition), offset, nil
}

// List returns the logical, unrequeued DLQ records in partition/offset order.
// Kafka has no cross-partition total order, so positional pagination is only a
// near-point-in-time view. Administrative IDs remain stable Kafka coordinates.
func (t *kafkaTransport) List(ctx context.Context, topology transport.Topology, offset, limit int) ([]transport.DeadLetter, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("kafka: DLQ offset must not be negative")
	}
	if limit < 1 || limit > maxDLQAdminBatch {
		return nil, fmt.Errorf("kafka: DLQ page limit must be between 1 and %d", maxDLQAdminBatch)
	}
	topic := t.dlqTopic(topology)
	starts, ends, err := t.logicalOffsets(ctx, topic, t.dlqAdminGroup(topology))
	if err != nil {
		return nil, err
	}
	partitions := sortedPartitions(ends)
	remaining, wanted := offset, limit
	result := make([]transport.DeadLetter, 0, limit)
	for _, partition := range partitions {
		if wanted == 0 {
			break
		}
		cursor, end := starts[partition], ends[partition]
		for cursor < end && wanted > 0 {
			batchLimit := maxDLQAdminBatch
			if remaining < maxDLQAdminBatch {
				batchLimit = min(maxDLQAdminBatch, remaining+wanted)
			}
			records, err := t.readDLQPartition(ctx, topic, partition, cursor, end, batchLimit)
			if err != nil {
				return nil, err
			}
			if len(records) == 0 {
				break
			}
			cursor = records[len(records)-1].Offset + 1
			for _, record := range records {
				if remaining > 0 {
					remaining--
					continue
				}
				decoded, err := decodeKafkaDLQRecord(record)
				if err != nil {
					return nil, err
				}
				result = append(result, decoded)
				wanted--
				if wanted == 0 {
					break
				}
			}
			if len(records) < batchLimit {
				break
			}
		}
	}
	return result, nil
}

// Requeue is publish-before-cursor-commit and therefore loss-safe but not
// atomic. IDs must form the complete logical prefix of every selected
// partition; Kafka cannot delete an arbitrary record from an append-only log.
func (t *kafkaTransport) Requeue(ctx context.Context, topology transport.Topology, messageIDs []string) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	if err := requireTargetedEventLayout(topology); err != nil {
		return err
	}
	if len(messageIDs) > maxDLQAdminBatch {
		return fmt.Errorf("kafka: DLQ requeue batch exceeds %d records", maxDLQAdminBatch)
	}
	t.dlqAdminMu.Lock()
	defer t.dlqAdminMu.Unlock()
	lease, err := t.acquireDLQAdminLease(ctx, topology)
	if err != nil {
		return err
	}
	defer lease.release()
	opCtx, generation, err := lease.operation()
	if err != nil {
		return err
	}
	topic := t.dlqTopic(topology)
	selected := make(map[int32]map[int64]struct{})
	maxOffset := make(map[int32]int64)
	for _, id := range messageIDs {
		partition, offset, err := parseKafkaDLQID(id)
		if err != nil {
			return err
		}
		if selected[partition] == nil {
			selected[partition] = make(map[int64]struct{})
			maxOffset[partition] = offset
		}
		selected[partition][offset] = struct{}{}
		maxOffset[partition] = max(maxOffset[partition], offset)
	}
	if len(selected) == 0 {
		return nil
	}
	starts, ends, err := t.logicalOffsets(opCtx, topic, t.dlqAdminGroup(topology))
	if err != nil {
		return err
	}
	var records []*kgo.Record
	for _, partition := range sortedSelectedPartitions(selected) {
		end, exists := ends[partition]
		if !exists || maxOffset[partition] >= end {
			return fmt.Errorf("kafka: DLQ record %s does not exist", kafkaDLQID(partition, maxOffset[partition]))
		}
		if maxOffset[partition] < starts[partition] {
			continue // an already advanced cursor makes a repeated request a no-op
		}
		prefix, err := t.readDLQPartition(opCtx, topic, partition, starts[partition], maxOffset[partition]+1, maxDLQAdminBatch+1)
		if err != nil {
			return err
		}
		if len(prefix) > maxDLQAdminBatch {
			return fmt.Errorf("kafka: DLQ requeue prefix exceeds %d records", maxDLQAdminBatch)
		}
		for _, record := range prefix {
			if _, ok := selected[partition][record.Offset]; !ok {
				return fmt.Errorf("kafka: DLQ requeue IDs must include partition %d logical prefix record %d", partition, record.Offset)
			}
			records = append(records, record)
		}
		if len(prefix) == 0 || prefix[len(prefix)-1].Offset != maxOffset[partition] {
			return fmt.Errorf("kafka: DLQ record %s does not exist", kafkaDLQID(partition, maxOffset[partition]))
		}
	}
	type requeueRecord struct {
		deadLetter  transport.DeadLetter
		destination string
	}
	prepared := make([]requeueRecord, 0, len(records))
	validatedGroups := make(map[string]struct{})
	for _, record := range records {
		deadLetter, err := decodeKafkaDLQRecord(record)
		if err != nil {
			return err
		}
		destination := t.mainTopic(topology)
		if topology.Kind == transport.EventTopology {
			if deadLetter.SourceGroup == "" {
				return fmt.Errorf("kafka: requeue DLQ record %s: source subscriber is unavailable", deadLetter.ID)
			}
			if _, ok := validatedGroups[deadLetter.SourceGroup]; !ok {
				if err := t.validateProvisionedSubscriber(opCtx, topology, deadLetter.SourceGroup); err != nil {
					return fmt.Errorf("kafka: requeue DLQ record %s: %w", deadLetter.ID, err)
				}
				validatedGroups[deadLetter.SourceGroup] = struct{}{}
			}
			destination = t.privateReadyTopic(topology, deadLetter.SourceGroup)
		}
		prepared = append(prepared, requeueRecord{deadLetter: deadLetter, destination: destination})
	}
	for _, record := range prepared {
		if err := lease.check(generation); err != nil {
			return err
		}
		message := record.deadLetter.Message
		removeMessageHeader(&message, kafkaTargetGroupHeader)
		if err := t.produceConfirmed(opCtx, messageRecord(record.destination, message)); err != nil {
			return fmt.Errorf("kafka: requeue DLQ record %s: %w", record.deadLetter.ID, err)
		}
	}
	offsets := make(kadm.Offsets)
	for partition, maximum := range maxOffset {
		if maximum >= starts[partition] {
			offsets.AddOffset(topic, partition, maximum+1, -1)
		}
	}
	return t.commitDLQCursor(opCtx, topology, lease, generation, offsets)
}

// Purge advances the logical admin cursor to a snapshot of the DLQ end
// offsets. Kafka retains the physical records according to topic retention.
func (t *kafkaTransport) Purge(ctx context.Context, topology transport.Topology) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	t.dlqAdminMu.Lock()
	defer t.dlqAdminMu.Unlock()
	lease, err := t.acquireDLQAdminLease(ctx, topology)
	if err != nil {
		return err
	}
	defer lease.release()
	opCtx, generation, err := lease.operation()
	if err != nil {
		return err
	}
	topic := t.dlqTopic(topology)
	ends, err := t.adm.ListEndOffsets(opCtx, topic)
	if err != nil {
		return fmt.Errorf("kafka: inspect DLQ end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("kafka: inspect DLQ end offsets: %w", err)
	}
	offsets := make(kadm.Offsets)
	for partition, end := range ends[topic] {
		if end.Offset < 0 {
			return fmt.Errorf("kafka: invalid DLQ end offset for partition %d", partition)
		}
		offsets.AddOffset(topic, partition, end.Offset, end.LeaderEpoch)
	}
	return t.commitDLQCursor(opCtx, topology, lease, generation, offsets)
}

func (t *kafkaTransport) Count(ctx context.Context, topology transport.Topology) (int64, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return 0, err
	}
	starts, ends, err := t.logicalOffsets(ctx, t.dlqTopic(topology), t.dlqAdminGroup(topology))
	if err != nil {
		return 0, err
	}
	return offsetDistance(starts, ends)
}

// Stats calculates Kafka lag from end offsets minus committed group offsets.
// Kafka does not expose the split between fetched/uncommitted and never-fetched
// records through group offsets, so all main-topic lag is reported as Pending.
func (t *kafkaTransport) Stats(ctx context.Context, topology transport.Topology) (transport.TopologyStats, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return transport.TopologyStats{}, err
	}
	main := t.mainTopic(topology)
	groups, err := t.statsGroups(ctx, topology)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	topics := []string{main, t.dlqTopic(topology)}
	for _, group := range groups {
		if topology.Kind == transport.EventTopology {
			topics = append(topics, group+".retry-ready")
		}
	}
	if topologyDelayEnabled(topology) {
		for _, tier := range delayTiers {
			topics = append(topics, t.delayTopic(topology, tier))
			if topology.Kind == transport.EventTopology {
				topics = append(topics, t.targetedDelayTopic(topology, tier))
			}
		}
	}
	snapshot, err := t.loadOffsetSnapshot(ctx, topics...)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	var mainLag int64
	for _, group := range groups {
		groupTopics := []string{main}
		if topology.Kind == transport.EventTopology {
			groupTopics = append(groupTopics, group+".retry-ready")
		}
		lag, err := t.groupLagFromSnapshot(ctx, snapshot, group, groupTopics...)
		if err != nil {
			return transport.TopologyStats{}, err
		}
		if mainLag > math.MaxInt64-lag {
			return transport.TopologyStats{}, fmt.Errorf("kafka: overflowing topology lag")
		}
		mainLag += lag
	}
	var delayed int64
	if topologyDelayEnabled(topology) {
		for _, tier := range delayTiers {
			lag, err := t.groupLagFromSnapshot(ctx, snapshot, fmt.Sprintf("%s-delay-%s-group", topology.Name(), tier), t.delayTopic(topology, tier))
			if err != nil {
				return transport.TopologyStats{}, err
			}
			if delayed > math.MaxInt64-lag {
				return transport.TopologyStats{}, fmt.Errorf("kafka: overflowing delayed count")
			}
			delayed += lag
			if topology.Kind == transport.EventTopology {
				lag, err = t.groupLagFromSnapshot(ctx, snapshot, fmt.Sprintf("%s-target-delay-%s-group", topology.Name(), tier), t.targetedDelayTopic(topology, tier))
				if err != nil {
					return transport.TopologyStats{}, err
				}
				if delayed > math.MaxInt64-lag {
					return transport.TopologyStats{}, fmt.Errorf("kafka: overflowing delayed count")
				}
				delayed += lag
			}
		}
	}
	dlq, err := t.groupLagFromSnapshot(ctx, snapshot, t.dlqAdminGroup(topology), t.dlqTopic(topology))
	if err != nil {
		return transport.TopologyStats{}, err
	}
	return transport.TopologyStats{
		Pending: mainLag, Delayed: delayed, DLQ: dlq, Lag: mainLag,
		LagKnown: true, OldestAgeKnown: mainLag == 0,
	}, nil
}

func (t *kafkaTransport) SubscriptionStats(ctx context.Context, sub transport.Subscription) (transport.TopologyStats, error) {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return transport.TopologyStats{}, fmt.Errorf("kafka: event subscriber required")
	}
	group := kafkaConsumerGroup(sub.Topology, sub.Group)
	main, private := t.mainTopic(sub.Topology), t.privateReadyTopic(sub.Topology, sub.Group)
	snapshot, err := t.loadOffsetSnapshot(ctx, main, private)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	lag, err := t.groupLagFromSnapshot(ctx, snapshot, group, main, private)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	// Targeted delay tiers are shared logs and Kafka has no per-header index.
	// Delayed is therefore unknown for one subscriber and is reported as zero,
	// rather than incorrectly attributing the entire shared backlog to it.
	return transport.TopologyStats{Pending: lag, Lag: lag, LagKnown: true, OldestAgeKnown: lag == 0}, nil
}

func (t *kafkaTransport) statsGroups(ctx context.Context, topology transport.Topology) ([]string, error) {
	if topology.Kind == transport.TaskTopology {
		return []string{kafkaConsumerGroup(topology, "")}, nil
	}
	privateTopics, err := t.registeredPrivateTopics(ctx, topology)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, len(privateTopics))
	for _, topic := range privateTopics {
		groups = append(groups, strings.TrimSuffix(topic, ".retry-ready"))
	}
	return groups, nil
}

func (t *kafkaTransport) groupLag(ctx context.Context, topic, group string) (int64, error) {
	starts, ends, err := t.logicalOffsets(ctx, topic, group)
	if err != nil {
		return 0, err
	}
	return offsetDistance(starts, ends)
}

func (t *kafkaTransport) logicalOffsets(ctx context.Context, topic, group string) (map[int32]int64, map[int32]int64, error) {
	snapshot, err := t.loadOffsetSnapshot(ctx, topic)
	if err != nil {
		return nil, nil, err
	}
	committed, err := t.adm.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka: fetch group %q offsets: %w", group, err)
	}
	return logicalOffsetsFromSnapshot(snapshot, committed, topic, group)
}

type kafkaOffsetSnapshot struct {
	starts kadm.ListedOffsets
	ends   kadm.ListedOffsets
}

func (t *kafkaTransport) loadOffsetSnapshot(ctx context.Context, topics ...string) (kafkaOffsetSnapshot, error) {
	startResponse, err := t.listOffsetsWhenLeaderReady(ctx, true, topics...)
	if err != nil {
		return kafkaOffsetSnapshot{}, fmt.Errorf("kafka: list start offsets: %w", err)
	}
	endResponse, err := t.listOffsetsWhenLeaderReady(ctx, false, topics...)
	if err != nil {
		return kafkaOffsetSnapshot{}, fmt.Errorf("kafka: list end offsets: %w", err)
	}
	return kafkaOffsetSnapshot{starts: startResponse, ends: endResponse}, nil
}

func (t *kafkaTransport) groupLagFromSnapshot(ctx context.Context, snapshot kafkaOffsetSnapshot, group string, topics ...string) (int64, error) {
	committed, err := t.adm.FetchOffsetsForTopics(ctx, group, topics...)
	if err != nil {
		return 0, fmt.Errorf("kafka: fetch group %q offsets: %w", group, err)
	}
	var total int64
	for _, topic := range topics {
		starts, ends, err := logicalOffsetsFromSnapshot(snapshot, committed, topic, group)
		if err != nil {
			return 0, err
		}
		lag, err := offsetDistance(starts, ends)
		if err != nil || total > math.MaxInt64-lag {
			return 0, fmt.Errorf("kafka: overflowing group %q lag", group)
		}
		total += lag
	}
	return total, nil
}

func logicalOffsetsFromSnapshot(snapshot kafkaOffsetSnapshot, committed kadm.OffsetResponses, topic, group string) (map[int32]int64, map[int32]int64, error) {
	starts := make(map[int32]int64, len(snapshot.ends[topic]))
	ends := make(map[int32]int64, len(snapshot.ends[topic]))
	for partition, end := range snapshot.ends[topic] {
		start, ok := snapshot.starts.Lookup(topic, partition)
		if !ok || start.Offset < 0 || end.Offset < start.Offset {
			return nil, nil, fmt.Errorf("kafka: invalid offset range for %s[%d]", topic, partition)
		}
		logicalStart := start.Offset
		if commit, ok := committed.Lookup(topic, partition); ok {
			if commit.Err != nil {
				return nil, nil, fmt.Errorf("kafka: fetch group %q offset for %s[%d]: %w", group, topic, partition, commit.Err)
			}
			if commit.At >= 0 {
				logicalStart = max(logicalStart, commit.At)
			}
		}
		if logicalStart > end.Offset {
			return nil, nil, fmt.Errorf("kafka: committed offset exceeds end for %s[%d]", topic, partition)
		}
		starts[partition], ends[partition] = logicalStart, end.Offset
	}
	return starts, ends, nil
}

func (t *kafkaTransport) listOffsetsWhenLeaderReady(ctx context.Context, start bool, topics ...string) (kadm.ListedOffsets, error) {
	backoff := 25 * time.Millisecond
	for {
		var (
			response kadm.ListedOffsets
			err      error
		)
		if start {
			response, err = t.adm.ListStartOffsets(ctx, topics...)
		} else {
			response, err = t.adm.ListEndOffsets(ctx, topics...)
		}
		if err == nil {
			err = response.Error()
		}
		if err == nil {
			return response, nil
		}
		if !isKafkaLeaderTransition(err) {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff = min(250*time.Millisecond, backoff*2)
	}
}

func isKafkaLeaderTransition(err error) bool {
	return errors.Is(err, kerr.LeaderNotAvailable) ||
		errors.Is(err, kerr.UnknownTopicOrPartition) ||
		errors.Is(err, kerr.NotLeaderForPartition)
}

func offsetDistance(starts, ends map[int32]int64) (int64, error) {
	var total int64
	for partition, end := range ends {
		start, ok := starts[partition]
		if !ok || start < 0 || end < start || total > math.MaxInt64-(end-start) {
			return 0, fmt.Errorf("kafka: invalid or overflowing offset range for partition %d", partition)
		}
		total += end - start
	}
	return total, nil
}

func (t *kafkaTransport) commitDLQCursor(ctx context.Context, topology transport.Topology, lease *dlqAdminLease, generation uint64, offsets kadm.Offsets) error {
	if len(offsets) == 0 {
		return nil
	}
	if err := lease.check(generation); err != nil {
		return err
	}
	if lease.client != nil {
		commitOffsets := make(map[string]map[int32]kgo.EpochOffset, len(offsets))
		for topic, partitions := range offsets {
			commitOffsets[topic] = make(map[int32]kgo.EpochOffset, len(partitions))
			for partition, offset := range partitions {
				commitOffsets[topic][partition] = kgo.EpochOffset{Epoch: offset.LeaderEpoch, Offset: offset.At}
			}
		}
		var commitErr error
		lease.client.CommitOffsetsSync(ctx, commitOffsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			if response == nil {
				commitErr = errors.New("missing broker response")
				return
			}
			for _, topic := range response.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						commitErr = err
						return
					}
				}
			}
		})
		if commitErr != nil {
			return fmt.Errorf("kafka: commit DLQ admin cursor: %w", commitErr)
		}
		return nil
	}
	responses, err := t.adm.CommitOffsets(ctx, t.dlqAdminGroup(topology), offsets)
	if err != nil {
		return fmt.Errorf("kafka: commit DLQ admin cursor: %w", err)
	}
	if err := responses.Error(); err != nil {
		return fmt.Errorf("kafka: commit DLQ admin cursor: %w", err)
	}
	return nil
}

func (t *kafkaTransport) readDLQPartition(ctx context.Context, topic string, partition int32, start, end int64, limit int) ([]*kgo.Record, error) {
	if t.scanDLQ != nil {
		return t.scanDLQ(ctx, topic, partition, start, end, limit)
	}
	return t.scanDLQPartition(ctx, topic, partition, start, end, limit)
}

func (t *kafkaTransport) scanDLQPartition(ctx context.Context, topic string, partition int32, start, end int64, limit int) ([]*kgo.Record, error) {
	if start >= end || limit <= 0 {
		return nil, nil
	}
	client, err := t.newReader(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partition: kgo.NewOffset().At(start)},
	}))
	if err != nil {
		return nil, fmt.Errorf("kafka: create DLQ reader: %w", err)
	}
	defer client.Close()
	records := make([]*kgo.Record, 0, min(limit, 64))
	position := start
	for position < end && len(records) < limit {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return nil, fmt.Errorf("kafka: read DLQ partition %d: %w", partition, err)
		}
		advanced := false
		fetches.EachPartition(func(fetched kgo.FetchTopicPartition) {
			if fetched.Topic != topic || fetched.Partition != partition || fetched.Err != nil {
				return
			}
			for _, record := range fetched.Records {
				if record.Offset >= end || len(records) >= limit {
					break
				}
				if record.Offset >= position {
					records = append(records, record)
					position = record.Offset + 1
					advanced = true
				}
			}
			if !advanced && fetched.HighWatermark >= end {
				position = end
				advanced = true
			}
		})
		if !advanced {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	return records, nil
}

func decodeKafkaDLQRecord(raw *kgo.Record) (transport.DeadLetter, error) {
	var record kafkaDLQRecord
	if err := json.Unmarshal(raw.Value, &record); err != nil || record.Version != kafkaDLQRecordVersion {
		return transport.DeadLetter{}, fmt.Errorf("kafka: DLQ record %s is malformed or unsupported", kafkaDLQID(raw.Partition, raw.Offset))
	}
	if len(record.Message.Payload) == 0 || record.Attempts < 0 || record.FailedAt < 0 || record.SourcePartition < 0 || record.SourceOffset < 0 || record.SourceTimestamp < 0 || record.SourceTopic == "" {
		return transport.DeadLetter{}, fmt.Errorf("kafka: DLQ record %s has invalid fields", kafkaDLQID(raw.Partition, raw.Offset))
	}
	return transport.DeadLetter{
		ID: kafkaDLQID(raw.Partition, raw.Offset), Message: record.Message,
		Reason: record.Reason, LastError: record.LastError, Attempts: record.Attempts,
		FailedAt: time.UnixMilli(record.FailedAt), Source: record.SourceTopic,
		SourceGroup: record.SourceGroup, SourcePosition: kafkaDLQID(record.SourcePartition, record.SourceOffset),
	}, nil
}

func sortedPartitions(offsets map[int32]int64) []int32 {
	partitions := make([]int32, 0, len(offsets))
	for partition := range offsets {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

func sortedSelectedPartitions(selected map[int32]map[int64]struct{}) []int32 {
	offsets := make(map[int32]int64, len(selected))
	for partition := range selected {
		offsets[partition] = 0
	}
	return sortedPartitions(offsets)
}
