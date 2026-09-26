package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Subscribe joins the consumer group specified in Config.ConsumerGroup and
// begins consuming records from topics. handler is invoked sequentially for
// each record; the offset is committed only after handler returns nil.
// A non-nil error from handler is logged but does not stop the consumer loop;
// the record is left uncommitted for the next redelivery cycle.
// Blocks until ctx is cancelled.
func (c *Client) Subscribe(ctx context.Context, topics []string, handler RecordHandler) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}

	kc.AddConsumeTopics(topics...)

	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("kafka: consumer started",
			"topics", topics,
			"group", c.cfg.ConsumerGroup,
		)
	}

	for {
		select {
		case <-ctx.Done():
			if c.cfg.Logger != nil {
				c.cfg.Logger.Info("kafka: consumer stopped", "topics", topics)
			}
			return nil
		default:
		}

		fetches := kc.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					continue
				}
				if c.cfg.Logger != nil {
					c.cfg.Logger.ErrorAsync("kafka: fetch error",
						"topic", fe.Topic,
						"partition", fe.Partition,
						"err", fe.Err.Error(),
					)
				}
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			record := recordToConsumeRecord(r)

			if handlerErr := handler(ctx, record); handlerErr != nil {
				if c.cfg.Logger != nil {
					c.cfg.Logger.ErrorAsync("kafka: handler failed — record will not be committed",
						"topic", r.Topic,
						"partition", r.Partition,
						"offset", r.Offset,
						"err", handlerErr.Error(),
					)
				}
				return
			}

			// Mark the record so the auto-commit goroutine (or manual commit)
			// will commit up to this offset.
			kc.MarkCommitRecords(r)
		})
	}
}

// SubscribeBatch is like Subscribe but delivers records in batches for higher
// throughput scenarios. handler receives a slice of records; the offsets of
// all records are committed only when handler returns nil for the entire batch.
// Blocks until ctx is cancelled.
func (c *Client) SubscribeBatch(ctx context.Context, topics []string, batchSize int, handler func(ctx context.Context, records []ConsumeRecord) error) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}

	if batchSize <= 0 {
		batchSize = 100
	}

	kc.AddConsumeTopics(topics...)

	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("kafka: batch consumer started",
			"topics", topics,
			"group", c.cfg.ConsumerGroup,
			"batch_size", batchSize,
		)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fetches := kc.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					continue
				}
				if c.cfg.Logger != nil {
					c.cfg.Logger.ErrorAsync("kafka: fetch error",
						"topic", fe.Topic,
						"partition", fe.Partition,
						"err", fe.Err.Error(),
					)
				}
			}
		}

		var (
			batch   []ConsumeRecord
			rawRecs []*kgo.Record
		)

		fetches.EachRecord(func(r *kgo.Record) {
			batch = append(batch, recordToConsumeRecord(r))
			rawRecs = append(rawRecs, r)

			if len(batch) >= batchSize {
				if handlerErr := handler(ctx, batch); handlerErr != nil {
					if c.cfg.Logger != nil {
						c.cfg.Logger.ErrorAsync("kafka: batch handler failed — records will not be committed",
							"batch_size", len(batch),
							"err", handlerErr.Error(),
						)
					}
				} else {
					kc.MarkCommitRecords(rawRecs...)
				}
				batch = batch[:0]
				rawRecs = rawRecs[:0]
			}
		})

		// Flush the remaining partial batch.
		if len(batch) > 0 {
			if handlerErr := handler(ctx, batch); handlerErr != nil {
				if c.cfg.Logger != nil {
					c.cfg.Logger.ErrorAsync("kafka: batch handler failed on partial batch",
						"batch_size", len(batch),
						"err", handlerErr.Error(),
					)
				}
			} else {
				kc.MarkCommitRecords(rawRecs...)
			}
		}
	}
}

// CommitOffsets manually commits all marked offsets to the broker.
// Use when ConsumerCommitInterval is 0 (auto-commit disabled).
func (c *Client) CommitOffsets(ctx context.Context) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	if err := kc.CommitMarkedOffsets(ctx); err != nil {
		return fmt.Errorf("kafka: CommitOffsets failed: %w", err)
	}
	return nil
}

// SeekToOffset seeks the consumer to a specific offset within a partition.
// Useful for replaying messages or recovering from processing errors.
// The seek takes effect on the next PollFetches call.
func (c *Client) SeekToOffset(topic string, partition int32, offset int64) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	kc.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {partition: {Epoch: -1, Offset: offset}},
	})
	return nil
}

// SeekToTime seeks the consumer to the earliest offset whose timestamp is
// greater than or equal to t within the given partition. It uses the kadm
// admin client to perform a ListOffsets request before updating the consumer
// position.
func (c *Client) SeekToTime(ctx context.Context, topic string, partition int32, t time.Time) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}

	adm := kadm.NewClient(kc)
	offsets, err := adm.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
	if err != nil {
		return fmt.Errorf("kafka: SeekToTime failed for topic %q partition %d: %w", topic, partition, err)
	}

	offsets.Each(func(o kadm.ListedOffset) {
		if o.Topic == topic && o.Partition == partition && o.Err == nil {
			kc.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				o.Topic: {o.Partition: {Epoch: o.LeaderEpoch, Offset: o.Offset}},
			})
		}
	})
	return nil
}

// PauseTopics temporarily pauses fetching from the specified topics.
// Fetches can be resumed by calling ResumeTopics with the same topic names.
func (c *Client) PauseTopics(topics ...string) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	kc.PauseFetchTopics(topics...)
	return nil
}

// ResumeTopics resumes fetching from topics previously paused via PauseTopics.
func (c *Client) ResumeTopics(topics ...string) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	kc.ResumeFetchTopics(topics...)
	return nil
}

// ---- internal helpers ----

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
