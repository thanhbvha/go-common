package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordMarker interface {
	MarkCommitRecords(...*kgo.Record)
}

type recordCommitter interface {
	CommitRecords(context.Context, ...*kgo.Record) error
}

type recordConsumer interface {
	recordCommitter
	PollRecords(context.Context, int) kgo.Fetches
	AllowRebalance()
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
}

var (
	errRebalanceRequested = errors.New("kafka: rebalance requested")
	errPartitionRevoked   = errors.New("kafka: partition revoked")
	errPartitionLost      = errors.New("kafka: partition lost")
)

func subscriptionCapacity(sub transport.Subscription) (int, error) {
	concurrency, prefetch := sub.Concurrency, sub.Prefetch
	if concurrency < 0 || prefetch < 0 {
		return 0, fmt.Errorf("kafka: negative subscription capacity")
	}
	if concurrency == 0 {
		concurrency = 10
	}
	if prefetch == 0 {
		prefetch = 20
	}
	if concurrency > int(^uint(0)>>1)-prefetch {
		return 0, fmt.Errorf("kafka: subscription capacity overflows int")
	}
	return concurrency + prefetch, nil
}

type partitionKey struct {
	topic     string
	partition int32
}

type offsetCommitRequest struct {
	ctx    context.Context
	record *kgo.Record
	result chan error
}

type offsetCancelRequest struct {
	request *offsetCommitRequest
}

type offsetRegisterRequest struct {
	records []*kgo.Record
	result  chan error
}

type offsetCloseRequest struct {
	result chan struct{}
}

type offsetOwnershipRequest struct {
	partitions map[string][]int32
	assigned   bool
	lost       bool
	result     chan error
}

type offsetTrackOwnershipRequest struct {
	result chan struct{}
}

type pendingOffset struct {
	record  *kgo.Record
	request *offsetCommitRequest
}

// offsetCoordinator is the only main-consumer component allowed to commit
// source offsets. It converts concurrent handler settlement into one ordered
// stream of synchronous, broker-confirmed commits per topic/partition.
type offsetCoordinator struct {
	committer recordCommitter
	commands  chan any
	done      chan struct{}
	closeOnce sync.Once
	doneOnce  sync.Once
	mu        sync.Mutex
	failure   error
}

func newOffsetCoordinator(committer recordCommitter, capacity int) *offsetCoordinator {
	if capacity < 1 {
		capacity = 1
	}
	c := &offsetCoordinator{
		committer: committer,
		commands:  make(chan any, capacity),
		done:      make(chan struct{}),
	}
	go func() {
		transport.Guard(func() { c.stop(transport.ErrRecoveredPanic) }, c.run)
	}()
	return c
}

func (c *offsetCoordinator) stop(err error) {
	c.mu.Lock()
	if c.failure == nil {
		c.failure = err
	}
	c.mu.Unlock()
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *offsetCoordinator) stoppedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	return context.Canceled
}

func (c *offsetCoordinator) register(ctx context.Context, records []*kgo.Record) error {
	request := offsetRegisterRequest{records: records, result: make(chan error, 1)}
	select {
	case c.commands <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
}

func (c *offsetCoordinator) settle(ctx context.Context, record *kgo.Record) error {
	request := &offsetCommitRequest{ctx: ctx, record: record, result: make(chan error, 1)}
	select {
	case c.commands <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		select {
		case c.commands <- offsetCancelRequest{request: request}:
		case <-c.done:
		}
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
}

func (c *offsetCoordinator) close() {
	c.closeOnce.Do(func() {
		request := offsetCloseRequest{result: make(chan struct{})}
		select {
		case c.commands <- request:
			select {
			case <-request.result:
			case <-c.done:
			}
		case <-c.done:
		}
	})
}

func (c *offsetCoordinator) trackOwnership() {
	request := offsetTrackOwnershipRequest{result: make(chan struct{})}
	select {
	case c.commands <- request:
		select {
		case <-request.result:
		case <-c.done:
		}
	case <-c.done:
	}
}

func (c *offsetCoordinator) assign(ctx context.Context, partitions map[string][]int32) error {
	return c.changeOwnership(ctx, partitions, true, false)
}

func (c *offsetCoordinator) revoke(ctx context.Context, partitions map[string][]int32, lost bool) error {
	return c.changeOwnership(ctx, partitions, false, lost)
}

func (c *offsetCoordinator) changeOwnership(ctx context.Context, partitions map[string][]int32, assigned, lost bool) error {
	request := offsetOwnershipRequest{partitions: partitions, assigned: assigned, lost: lost, result: make(chan error, 1)}
	select {
	case c.commands <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
}

func (c *offsetCoordinator) run() {
	partitions := make(map[partitionKey][]*pendingOffset)
	entries := make(map[*kgo.Record]*pendingOffset)
	fenced := make(map[*kgo.Record]error)
	owned := make(map[partitionKey]bool)
	trackOwnership := false

	respond := func(request *offsetCommitRequest, err error) {
		if request != nil {
			request.result <- err
		}
	}
	flush := func(key partitionKey) {
		queue := partitions[key]
		count := 0
		for count < len(queue) && queue[count].request != nil {
			if err := queue[count].request.ctx.Err(); err != nil {
				respond(queue[count].request, err)
				queue[count].request = nil
				break
			}
			count++
		}
		if count == 0 {
			return
		}
		toCommit := queue[count-1].record
		err := c.committer.CommitRecords(queue[count-1].request.ctx, toCommit)
		for _, entry := range queue[:count] {
			respond(entry.request, err)
			if err == nil {
				delete(entries, entry.record)
			} else {
				entry.request = nil
			}
		}
		if err == nil {
			queue = queue[count:]
			if len(queue) == 0 {
				delete(partitions, key)
			} else {
				partitions[key] = queue
			}
		}
	}

	for command := range c.commands {
		switch request := command.(type) {
		case offsetRegisterRequest:
			var err error
			tails := make(map[partitionKey]int64)
			seen := make(map[*kgo.Record]struct{}, len(request.records))
			for _, record := range request.records {
				if record == nil {
					err = fmt.Errorf("kafka: cannot coordinate a nil record")
					break
				}
				key := partitionKey{record.Topic, record.Partition}
				if trackOwnership && !owned[key] {
					err = fmt.Errorf("%w: %s[%d]", errPartitionRevoked, record.Topic, record.Partition)
					break
				}
				queue := partitions[key]
				tail, hasTail := tails[key]
				if !hasTail && len(queue) > 0 {
					tail, hasTail = queue[len(queue)-1].record.Offset, true
				}
				_, duplicate := seen[record]
				if _, exists := entries[record]; exists || duplicate || (hasTail && record.Offset <= tail) {
					err = fmt.Errorf("kafka: non-increasing offset %d for %s[%d]", record.Offset, record.Topic, record.Partition)
					break
				}
				tails[key] = record.Offset
				seen[record] = struct{}{}
			}
			if err == nil {
				for _, record := range request.records {
					key := partitionKey{record.Topic, record.Partition}
					entry := &pendingOffset{record: record}
					partitions[key] = append(partitions[key], entry)
					entries[record] = entry
				}
			}
			request.result <- err
		case offsetTrackOwnershipRequest:
			trackOwnership = true
			close(request.result)
		case offsetOwnershipRequest:
			for topic, topicPartitions := range request.partitions {
				for _, partition := range topicPartitions {
					key := partitionKey{topic, partition}
					if request.assigned {
						owned[key] = true
						continue
					}
					delete(owned, key)
					partitionErr := errPartitionRevoked
					if request.lost {
						partitionErr = errPartitionLost
					}
					for _, entry := range partitions[key] {
						fenceErr := fmt.Errorf("%w: %s[%d]", partitionErr, topic, partition)
						respond(entry.request, fenceErr)
						fenced[entry.record] = fenceErr
						delete(entries, entry.record)
					}
					delete(partitions, key)
				}
			}
			request.result <- nil
		case *offsetCommitRequest:
			entry, ok := entries[request.record]
			if !ok {
				if err, fencedRecord := fenced[request.record]; fencedRecord {
					delete(fenced, request.record)
					respond(request, err)
					continue
				}
				respond(request, fmt.Errorf("kafka: offset is not registered for coordination"))
				continue
			}
			if entry.request != nil {
				respond(request, fmt.Errorf("kafka: offset settlement is already pending"))
				continue
			}
			entry.request = request
			flush(partitionKey{request.record.Topic, request.record.Partition})
		case offsetCancelRequest:
			entry, ok := entries[request.request.record]
			if ok && entry.request == request.request {
				entry.request = nil
				respond(request.request, request.request.ctx.Err())
			}
		case offsetCloseRequest:
			for _, entry := range entries {
				respond(entry.request, context.Canceled)
			}
			close(request.result)
			c.stop(context.Canceled)
			return
		}
	}
}

// dispatchBatch keeps at most one delivery per topic/partition in the engine.
// Callback return means acceptance, not handler completion: only successful
// settlement unlocks the next record. No per-record or per-partition goroutines
// are needed. A failed settlement ends intake, leaving later offsets untouched.
func (t *kafkaTransport) dispatchBatch(ctx context.Context, topo transport.Topology, group string, coordinator *offsetCoordinator, rebalances *rebalanceController, records []*kgo.Record, deliver func(transport.Delivery)) (map[string]map[int32]kgo.EpochOffset, error) {
	queues := make(map[partitionKey][]*kgo.Record)
	var keys []partitionKey
	for _, record := range records {
		key := partitionKey{record.Topic, record.Partition}
		if _, exists := queues[key]; !exists {
			keys = append(keys, key)
		}
		queues[key] = append(queues[key], record)
	}
	type completion struct {
		key partitionKey
		err error
	}
	// Notifications never block settlement, including after cancellation or a
	// failed session. Each delivery sends at most once into this bounded channel.
	done := make(chan completion, len(records))
	dispatch := func(key partitionKey) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rebalances.requested() {
			return errRebalanceRequested
		}
		record := queues[key][0]
		if err := coordinator.register(ctx, []*kgo.Record{record}); err != nil {
			return err
		}
		queues[key] = queues[key][1:]
		var env eventbus.Envelope
		count := 1
		if json.Unmarshal(record.Value, &env) == nil {
			count = env.Attempt
			if env.V == 2 {
				count++
			}
			count = max(1, count)
		}
		var once sync.Once
		deliver(&kafkaDelivery{
			transport: t, topo: topo, coordinator: coordinator, record: record,
			msg:   deliveryMessage(record),
			count: count, group: group,
			completed: func(err error) { once.Do(func() { done <- completion{key, err} }) },
		})
		return nil
	}
	inFlight := 0
	stopping := rebalances.requested()
	for _, key := range keys {
		if stopping {
			break
		}
		if err := dispatch(key); err != nil {
			if errors.Is(err, errRebalanceRequested) {
				stopping = true
				break
			}
			return nil, err
		}
		inFlight++
	}
	for inFlight > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-rebalances.requestedSignal():
			stopping = true
		case result := <-done:
			inFlight--
			if result.err != nil {
				return nil, fmt.Errorf("kafka: partition settlement failed: %w", result.err)
			}
			if rebalances.requested() {
				stopping = true
			}
			if !stopping && len(queues[result.key]) > 0 {
				if err := dispatch(result.key); err != nil {
					if errors.Is(err, errRebalanceRequested) {
						stopping = true
						continue
					}
					return nil, err
				}
				inFlight++
			}
		}
	}
	if stopping {
		return rewindOffsets(queues), errRebalanceRequested
	}
	return nil, nil
}

func deliveryMessage(record *kgo.Record) transport.Message {
	msg := recordMessage(record)
	removeMessageHeader(&msg, kafkaRunAtHeader)
	removeMessageHeader(&msg, kafkaTargetGroupHeader)
	return msg
}

func rewindOffsets(queues map[partitionKey][]*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	var offsets map[string]map[int32]kgo.EpochOffset
	for key, records := range queues {
		if len(records) == 0 {
			continue
		}
		if offsets == nil {
			offsets = make(map[string]map[int32]kgo.EpochOffset)
		}
		if offsets[key.topic] == nil {
			offsets[key.topic] = make(map[int32]kgo.EpochOffset)
		}
		first := records[0]
		offsets[key.topic][key.partition] = kgo.NewOffset().At(first.Offset).WithEpoch(first.LeaderEpoch).EpochOffset()
	}
	return offsets
}
