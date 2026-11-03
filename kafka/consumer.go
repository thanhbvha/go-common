package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Subscribe handles records sequentially on an independently owned consumer.
// The first handler, poll, or commit error stops the subscription. Successful
// records preceding a handler error are synchronously committed; the failed
// record and later records remain available after restarting the subscription.
// Handlers must be idempotent and honor ctx. Retry and DLQ belong in eventbus.
func (c *Client) Subscribe(ctx context.Context, topics []string, handler RecordHandler) error {
	if handler == nil {
		return errors.New("kafka: nil record handler")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	consumer, err := c.openSubscription(topics)
	if err != nil {
		return err
	}
	return consumeSubscription(ctx, consumer, 100, func(ctx context.Context, records []*kgo.Record) (int, error) {
		for i, record := range records {
			if err := ctx.Err(); err != nil {
				return i, err
			}
			if err := handler(ctx, recordToConsumeRecord(record)); err != nil {
				return i, fmt.Errorf("kafka: handler topic=%q partition=%d offset=%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
		}
		return len(records), nil
	})
}

// SubscribeBatch commits a bounded batch only after handler returns nil.
// On any error it closes its consumer and returns without automatic retry.
// A failed batch may have partial application side effects; handlers must be
// idempotent because all records in that batch remain eligible for redelivery.
func (c *Client) SubscribeBatch(ctx context.Context, topics []string, batchSize int, handler func(context.Context, []ConsumeRecord) error) error {
	if handler == nil {
		return errors.New("kafka: nil batch handler")
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	consumer, err := c.openSubscription(topics)
	if err != nil {
		return err
	}
	return consumeSubscription(ctx, consumer, batchSize, func(ctx context.Context, records []*kgo.Record) (int, error) {
		batch := make([]ConsumeRecord, len(records))
		for i, record := range records {
			batch[i] = recordToConsumeRecord(record)
		}
		if err := handler(ctx, batch); err != nil {
			return 0, fmt.Errorf("kafka: batch handler: %w", err)
		}
		return len(records), nil
	})
}

type subscriptionConsumer interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
}

func (c *Client) openSubscription(topics []string) (*kgo.Client, error) {
	if strings.TrimSpace(c.cfg.ConsumerGroup) == "" || len(topics) == 0 {
		return nil, errors.New("kafka: consumer group and topics are required")
	}
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return nil, errors.New("kafka: empty consumer topic")
		}
	}
	if _, err := c.requireConnected(); err != nil {
		return nil, err
	}
	factory, err := NewNativeClientFactory(c.cfg)
	if err != nil {
		return nil, err
	}
	return factory.NewManualConsumer(kgo.ConsumerGroup(c.cfg.ConsumerGroup), kgo.ConsumeTopics(topics...), kgo.BlockRebalanceOnPoll())
}

// consumeSubscription owns consumer until return. At most one bounded poll is
// in flight, and assignment remains fenced until handlers and commits finish.
func consumeSubscription(ctx context.Context, consumer subscriptionConsumer, batchSize int, handle func(context.Context, []*kgo.Record) (int, error)) error {
	defer consumer.CloseAllowingRebalance()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches := consumer.PollRecords(ctx, batchSize)
		if err := fetches.Err(); err != nil {
			return err
		}
		if fetches.IsClientClosed() {
			return ErrClosed
		}
		records := fetches.Records()
		if len(records) == 0 {
			consumer.AllowRebalance()
			continue
		}
		accepted, handleErr := handle(ctx, records)
		if accepted > 0 {
			if err := consumer.CommitRecords(ctx, records[:accepted]...); err != nil {
				return errors.Join(handleErr, fmt.Errorf("kafka: commit processed records: %w", err))
			}
		}
		if handleErr != nil {
			return handleErr
		}
		consumer.AllowRebalance()
	}
}

// recordToConsumeRecord converts a *kgo.Record to a ConsumeRecord.
func recordToConsumeRecord(r *kgo.Record) ConsumeRecord {
	rec := ConsumeRecord{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
	if len(r.Headers) > 0 {
		rec.Headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			rec.Headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}
	return rec
}
