package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Produce sends a single Message to Kafka and blocks until the broker
// acknowledges the write or the context is cancelled.
// The ProducerAcks setting controls the acknowledgement level.
func (c *Client) Produce(ctx context.Context, msg Message) (ProduceResult, error) {
	kc, err := c.requireConnected()
	if err != nil {
		return ProduceResult{}, err
	}

	rec := messageToRecord(msg)

	var result ProduceResult
	var produceErr error

	done := make(chan struct{})
	kc.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			produceErr = fmt.Errorf("kafka: produce failed for topic %q: %w", msg.Topic, err)
		} else {
			result = ProduceResult{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp,
			}
		}
		close(done)
	})

	select {
	case <-done:
		return result, produceErr
	case <-ctx.Done():
		return ProduceResult{}, fmt.Errorf("kafka: produce context cancelled for topic %q: %w", msg.Topic, ctx.Err())
	}
}

// ProduceBatch sends multiple Messages to Kafka concurrently and waits until
// all produce callbacks complete. It returns a result for each message in the
// same order and the first error encountered (if any).
func (c *Client) ProduceBatch(ctx context.Context, msgs []Message) ([]ProduceResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	kc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}

	return produceBatch(ctx, kc, msgs)
}

type batchProducer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

func produceBatch(ctx context.Context, producer batchProducer, msgs []Message) ([]ProduceResult, error) {
	type outcome struct {
		index  int
		result ProduceResult
		err    error
	}
	completed := make(chan outcome, len(msgs))
	results := make([]ProduceResult, len(msgs))
	errs := make([]error, len(msgs))
	for i, msg := range msgs {
		producer.Produce(ctx, messageToRecord(msg), func(record *kgo.Record, err error) {
			item := outcome{index: i}
			if err != nil {
				item.err = fmt.Errorf("kafka: produce topic=%q message=%d: %w", msg.Topic, i, err)
			} else {
				item.result = ProduceResult{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Timestamp: record.Timestamp}
			}
			completed <- item
		})
	}
	for range msgs {
		select {
		case item := <-completed:
			results[item.index], errs[item.index] = item.result, item.err
		case <-ctx.Done():
			return results, fmt.Errorf("kafka: ProduceBatch context cancelled; acceptance may be ambiguous: %w", ctx.Err())
		}
	}
	for _, err := range errs {
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

// ProduceAsync sends a Message to Kafka without blocking. The provided callback
// is invoked from a franz-go internal goroutine once the produce attempt
// completes (successfully or with an error). The callback must not block.
func (c *Client) ProduceAsync(msg Message, callback func(ProduceResult, error)) {
	kc, err := c.requireConnected()
	if err != nil {
		if callback != nil {
			callback(ProduceResult{}, err)
		}
		return
	}

	rec := messageToRecord(msg)
	kc.Produce(context.Background(), rec, func(r *kgo.Record, produceErr error) {
		if callback == nil {
			return
		}
		if produceErr != nil {
			callback(ProduceResult{}, fmt.Errorf("kafka: async produce failed for topic %q: %w", msg.Topic, produceErr))
			return
		}
		callback(ProduceResult{
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Timestamp: r.Timestamp,
		}, nil)
	})
}

// ProduceJSON marshals value to JSON and produces it to topic using key as
// the Kafka message key. Optional headers are attached to the record.
// Returns the broker-assigned ProduceResult on success.
func (c *Client) ProduceJSON(ctx context.Context, topic string, key string, value any, headers ...Header) (ProduceResult, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return ProduceResult{}, fmt.Errorf("kafka: ProduceJSON marshal failed for topic %q: %w", topic, err)
	}

	msg := Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   data,
		Headers: headers,
	}
	return c.Produce(ctx, msg)
}

// Flush waits until all buffered produce records have been sent to the broker
// or the context is cancelled. Call before graceful shutdown to avoid data loss.
func (c *Client) Flush(ctx context.Context) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	if err := kc.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush failed: %w", err)
	}
	return nil
}

// ---- internal helpers ----

// messageToRecord snapshots caller-owned bytes for native asynchronous use.
// Native callbacks may outlive a canceled synchronous publication.
func messageToRecord(msg Message) *kgo.Record {
	rec := &kgo.Record{
		Topic: msg.Topic,
		Key:   bytes.Clone(msg.Key),
		Value: bytes.Clone(msg.Value),
	}

	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp
	} else {
		rec.Timestamp = time.Now()
	}

	if len(msg.Headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, len(msg.Headers))
		for i, h := range msg.Headers {
			rec.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: bytes.Clone(h.Value)}
		}
	}

	if msg.Partition >= 0 {
		rec.Partition = msg.Partition
	}

	return rec
}
