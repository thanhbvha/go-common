package kafka

import (
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

	results := make([]ProduceResult, len(msgs))
	errs := make([]error, len(msgs))
	done := make(chan struct{}, len(msgs))

	for i, msg := range msgs {
		rec := messageToRecord(msg)
		idx := i
		kc.Produce(ctx, rec, func(r *kgo.Record, callbackErr error) {
			if callbackErr != nil {
				errs[idx] = fmt.Errorf("kafka: produce failed for topic %q message %d: %w", r.Topic, idx, callbackErr)
			} else {
				results[idx] = ProduceResult{
					Topic:     r.Topic,
					Partition: r.Partition,
					Offset:    r.Offset,
					Timestamp: r.Timestamp,
				}
			}
			done <- struct{}{}
		})
	}

	// Wait for all callbacks or context cancellation.
	for range msgs {
		select {
		case <-done:
		case <-ctx.Done():
			return results, fmt.Errorf("kafka: ProduceBatch context cancelled: %w", ctx.Err())
		}
	}

	// Return the first non-nil error encountered.
	for _, e := range errs {
		if e != nil {
			return results, e
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

// messageToRecord converts a Message to a *kgo.Record for franz-go.
func messageToRecord(msg Message) *kgo.Record {
	rec := &kgo.Record{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	}

	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp
	} else {
		rec.Timestamp = time.Now()
	}

	if len(msg.Headers) > 0 {
		rec.Headers = make([]kgo.RecordHeader, len(msg.Headers))
		for i, h := range msg.Headers {
			rec.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}

	if msg.Partition >= 0 {
		rec.Partition = msg.Partition
	}

	return rec
}
