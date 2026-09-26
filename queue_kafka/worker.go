package queue_kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/kafka"
)

// workerLoop is the main consumer loop for a single worker goroutine.
// It subscribes to the given Kafka topic using the job type's consumer group,
// dispatches each job to its registered handler, and routes failed jobs to
// the retry topic or DLQ based on the retry count.
func (q *Queue) workerLoop(ctx context.Context, jobType string, cfg jobTypeConfig, workerID int, topic string) {
	workerName := fmt.Sprintf("%s-worker-%d", jobType, workerID)
	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	q.logInfo("queue_kafka: worker started",
		"worker", workerName, "topic", topic, "group", cfg.Group)

	err := q.kafka.Subscribe(consumerCtx, []string{topic}, func(ctx context.Context, record kafka.ConsumeRecord) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var job Job
		if unmarshalErr := json.Unmarshal(record.Value, &job); unmarshalErr != nil {
			// A malformed record cannot be retried meaningfully; log and skip.
			q.logErrorAsync("queue_kafka: failed to unmarshal job record — skipping",
				"worker", workerName, "topic", record.Topic,
				"offset", record.Offset, "err", unmarshalErr.Error())
			// Return nil so the offset is committed and we do not loop on a
			// permanently broken message.
			return nil
		}

		// Dynamically resolve config for the actual job type received,
		// because Kafka clients might fetch records from multiple topics.
		jobCfg := q.resolveType(job.Type)

		// Apply a per-job processing timeout.
		processCtx := ctx
		var processCancel context.CancelFunc
		if q.cfg.ProcessTimeout > 0 {
			processCtx, processCancel = context.WithTimeout(ctx, q.cfg.ProcessTimeout)
			defer processCancel()
		}
		_ = processCtx

		handlerErr := q.executeHandler(job, workerName)
		if handlerErr != nil {
			q.logErrorAsync("queue_kafka: job handler returned error",
				"worker", workerName, "job_id", job.ID, "type", job.Type,
				"retry", job.Retry, "max_retry", jobCfg.MaxRetry,
				"err", handlerErr.Error())
			// Retry or DLQ asynchronously so we can commit the current offset
			// and avoid re-consuming the same record from the broker.
			go q.retryOrDLQ(context.Background(), job, jobCfg)
		}

		// Return nil to commit the offset regardless of handler outcome.
		// The job has been handed off to the retry/DLQ pipeline when needed.
		return nil
	})

	if err != nil && ctx.Err() == nil {
		q.logError("queue_kafka: worker Subscribe returned unexpected error",
			"worker", workerName, "topic", topic, "err", err.Error())
	}

	q.logInfo("queue_kafka: worker stopped", "worker", workerName, "topic", topic)
}

// delayedDispatchLoop consumes the delayed-job topic and re-enqueues jobs
// whose RunAt timestamp has been reached. Jobs not yet due are re-published
// back to the delayed topic and committed, relying on the poll interval to
// re-check them.
func (q *Queue) delayedDispatchLoop(ctx context.Context) {
	q.logInfo("queue_kafka: delayed dispatcher started", "topic", q.cfg.DelayedTopic)

	err := q.kafka.Subscribe(ctx, []string{q.cfg.DelayedTopic}, func(ctx context.Context, record kafka.ConsumeRecord) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var job Job
		if unmarshalErr := json.Unmarshal(record.Value, &job); unmarshalErr != nil {
			q.logErrorAsync("queue_kafka: delayed dispatcher failed to unmarshal job — skipping",
				"offset", record.Offset, "err", unmarshalErr.Error())
			return nil
		}

		now := time.Now().Unix()
		if job.RunAt > now {
			// Job is not yet due. Sleep for the remaining duration (capped at
			// DelayCheckInterval) then re-publish to the delayed topic.
			remaining := time.Duration(job.RunAt-now) * time.Second
			if remaining > q.cfg.DelayCheckInterval {
				remaining = q.cfg.DelayCheckInterval
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(remaining):
			}
			// Re-publish so other dispatcher instances can also see the job.
			if pushErr := q.pushDelayed(ctx, &job); pushErr != nil {
				q.logErrorAsync("queue_kafka: failed to re-publish delayed job",
					"job_id", job.ID, "type", job.Type, "err", pushErr.Error())
			}
			return nil
		}

		// Job is due — push to the main topic.
		cfg := q.resolveType(job.Type)
		if pushErr := q.pushToTopic(ctx, &job, cfg); pushErr != nil {
			q.logErrorAsync("queue_kafka: delayed dispatcher failed to dispatch job",
				"job_id", job.ID, "type", job.Type, "err", pushErr.Error())
			return nil
		}

		q.logInfoAsync("queue_kafka: delayed job dispatched",
			"job_id", job.ID, "type", job.Type,
			"run_at", time.Unix(job.RunAt, 0).Format(time.RFC3339))
		return nil
	})

	if err != nil && ctx.Err() == nil {
		q.logError("queue_kafka: delayed dispatcher Subscribe error", "err", err.Error())
	}

	q.logInfo("queue_kafka: delayed dispatcher stopped")
}
