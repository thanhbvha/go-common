package queue_nats

import (
	"context"
	"github.com/goccy/go-json"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/nats"
)

// workerLoop is the main consumer loop for a single worker goroutine.
// It fetches batches from the assigned stream/durable-consumer and dispatches each job to
// its registered handler. Failed jobs are re-delivered by NATS or routed to the DLQ.
func (q *Queue) workerLoop(ctx context.Context, jobType string, cfg jobTypeConfig, workerID int) {
	consumerName := fmt.Sprintf("%s-worker-%d", jobType, workerID)
	streamName := cfg.StreamName
	group := cfg.Group
	subject := fmt.Sprintf("%s.%s", q.cfg.StreamPrefix, jobType)

	// Create or get pull subscription
	sub, err := q.nats.PullSubscribe(subject, group, nats.WithManualAck())
	if err != nil {
		q.logErrorAsync("queue: failed to create pull subscription",
			"worker", consumerName, "stream", streamName, "group", group, "err", err.Error())
		return
	}
	defer sub.Unsubscribe()

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	for {
		select {
		case <-ctx.Done():
			q.logInfoAsync("queue: worker shutting down",
				"worker", consumerName, "stream", streamName, "group", group)
			return
		default:
		}

		messages, err := q.nats.Fetch(sub, batchSize, nats.WithFetchTimeout(2*time.Second))
		if err != nil {
			if err == context.Canceled || ctx.Err() != nil {
				return
			}
			// Timeout is expected if no messages are available
			if err.Error() == "nats: timeout" {
				continue
			}
			q.logErrorAsync("queue: error fetching from stream",
				"worker", consumerName, "stream", streamName, "err", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return
			default:
			}

			job, ok := q.parseJobMessage(msg, consumerName, streamName)
			if !ok {
				// Unparseable — ACK to prevent infinite redelivery.
				msg.Ack()
				continue
			}

			// Synchronize job Retry with NATS delivery count
			job.Retry = int(msg.NumDelivered) - 1

			if err := q.executeHandler(*job, consumerName); err != nil {
				q.logErrorAsync("queue: job failed",
					"worker", consumerName, "job_id", job.ID, "type", job.Type, "err", err.Error())
				q.retryOrDLQ(ctx, *job, msg, cfg)
				continue
			}
			msg.Ack()
		}
	}
}

// delayedDispatchLoop fetches messages from the delayed stream.
// If a message is not yet due, it Naks with a delay.
// If it is due, it pushes the message to the target stream and Acks it.
func (q *Queue) delayedDispatchLoop(ctx context.Context) {
	group := "delayed-dispatcher"
	consumerName := group
	streamName := "queue_delayed"
	subject := q.cfg.DelayedStreamSubject

	sub, err := q.nats.PullSubscribe(subject, group, nats.WithManualAck())
	if err != nil {
		q.logErrorAsync("queue: failed to create delayed pull subscription", "err", err.Error())
		return
	}
	defer sub.Unsubscribe()

	const batchSize = 50

	for {
		select {
		case <-ctx.Done():
			q.logInfoAsync("queue: delayed dispatcher shutting down")
			return
		default:
		}

		messages, err := q.nats.Fetch(sub, batchSize, nats.WithFetchTimeout(2*time.Second))
		if err != nil {
			if err == context.Canceled || ctx.Err() != nil {
				return
			}
			if err.Error() == "nats: timeout" {
				continue
			}
			q.logErrorAsync("queue: error fetching delayed stream", "err", err.Error())
			time.Sleep(1 * time.Second)
			continue
		}

		now := time.Now().Unix()

		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return
			default:
			}

			job, ok := q.parseJobMessage(msg, consumerName, streamName)
			if !ok {
				msg.Ack()
				continue
			}

			if job.RunAt > now {
				// Not due yet, Nak with delay so NATS redelivers it later
				delaySec := job.RunAt - now
				if delaySec > 0 {
					msg.NakWithDelay(time.Duration(delaySec) * time.Second)
					continue
				}
			}

			// Job is due! Push to target stream
			targetSubject := fmt.Sprintf("%s.%s", q.cfg.StreamPrefix, job.Type)
			
			// Marshal again to update any fields if necessary, or just use raw data
			// We push the updated job so RunAt is retained if needed, though we can just push original Data.
			jobBytes, _ := json.Marshal(job)

			if _, err := q.nats.JSPublish(ctx, targetSubject, jobBytes); err != nil {
				// Fallback
				defaultSubject := fmt.Sprintf("%s.default", q.cfg.StreamPrefix)
				if _, errDefault := q.nats.JSPublish(ctx, defaultSubject, jobBytes); errDefault != nil {
					q.logErrorAsync("queue: failed to dispatch delayed job", "err", err.Error())
					// Nak so we can retry dispatching later
					msg.NakWithDelay(5 * time.Second)
					continue
				}
			}
			
			// Successfully dispatched
			msg.Ack()
		}
	}
}

// retryOrDLQ routes job to the DLQ when MaxRetry is exhausted, otherwise Naks for redelivery.
func (q *Queue) retryOrDLQ(ctx context.Context, job Job, msg *nats.Msg, cfg jobTypeConfig) {
	if job.MaxRetry == 0 {
		job.MaxRetry = cfg.MaxRetry
	}

	if job.Retry < job.MaxRetry {
		q.logInfoAsync("queue: job scheduled for retry via NATS redelivery",
			"job_id", job.ID, "type", job.Type,
			"retry", job.Retry+1, "max_retry", job.MaxRetry)
		// Nak triggers redelivery. NakWithDelay can be used for exponential backoff if desired.
		msg.NakWithDelay(time.Duration(job.Retry+1) * 10 * time.Second)
		return
	}

	// Exhausted retries, move to DLQ
	jobBytes, _ := json.Marshal(job)
	dlqSubject := q.cfg.DLQStreamName

	if _, err := q.nats.JSPublish(ctx, dlqSubject, jobBytes); err == nil {
		q.logInfoAsync("queue: job moved to DLQ",
			"job_id", job.ID, "type", job.Type,
			"retry", job.Retry, "max_retry", job.MaxRetry)
		msg.Term() // Dead-lettered in current stream
	} else {
		q.logErrorAsync("queue: failed to move job to DLQ, will retry", "err", err.Error())
		msg.NakWithDelay(1 * time.Minute)
	}
}

// parseJobMessage extracts and unmarshals a Job from a NATS Msg.
func (q *Queue) parseJobMessage(msg *nats.Msg, consumer, streamName string) (*Job, bool) {
	var job Job
	if err := json.Unmarshal(msg.Data, &job); err != nil {
		q.logErrorAsync("queue: failed to unmarshal job",
			"consumer", consumer, "stream", streamName, "sequence", msg.Sequence, "err", err.Error())
		return nil, false
	}
	return &job, true
}
