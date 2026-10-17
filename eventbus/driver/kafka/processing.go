package kafka

import (
	"context"
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

type recordConsumer interface {
	recordMarker
	PollRecords(context.Context, int) kgo.Fetches
}

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

// dispatchBatch keeps at most one delivery per topic/partition in the engine.
// Callback return means acceptance, not handler completion: only successful
// settlement unlocks the next record. No per-record or per-partition goroutines
// are needed. A failed settlement ends intake, leaving later offsets untouched.
func (t *kafkaTransport) dispatchBatch(ctx context.Context, topo transport.Topology, cl recordMarker, records []*kgo.Record, deliver func(transport.Delivery)) error {
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
		record := queues[key][0]
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
			transport: t, topo: topo, cl: cl, record: record,
			msg:       transport.Message{Payload: record.Value, OrderingKey: string(record.Key)},
			count:     count,
			completed: func(err error) { once.Do(func() { done <- completion{key, err} }) },
		})
		return nil
	}
	for _, key := range keys {
		if err := dispatch(key); err != nil {
			return err
		}
	}
	for remaining := len(records); remaining > 0; remaining-- {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-done:
			if result.err != nil {
				return fmt.Errorf("kafka: partition settlement failed: %w", result.err)
			}
			if len(queues[result.key]) > 0 {
				if err := dispatch(result.key); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
