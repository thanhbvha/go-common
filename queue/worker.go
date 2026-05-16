package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// workerLoop is the main consumer loop for a single worker goroutine.
// It reads batches from the assigned stream/group and dispatches each job to
// its registered handler. Failed jobs are retried or routed to the DLQ.
func (q *Queue) workerLoop(ctx context.Context, jobType string, cfg jobTypeConfig, workerID int) {
	consumer := fmt.Sprintf("%s-worker-%d", jobType, workerID)
	streamKey := q.redis.BuildKey(cfg.StreamKey)
	group := cfg.Group

	for {
		select {
		case <-ctx.Done():
			q.logInfo("queue: worker shutting down",
				"worker", consumer, "stream", streamKey, "group", group)
			return
		default:
		}

		batchSize := cfg.BatchSize
		if batchSize <= 0 {
			batchSize = 1
		}

		streams := []string{streamKey, ">"}
		messages, err := q.redis.XReadGroup(ctx, group, consumer, streams, batchSize, 100*time.Millisecond)
		if err != nil {
			if err == context.Canceled || ctx.Err() != nil {
				return
			}
			if err == goredis.Nil {
				continue
			}
			q.logError("queue: error reading from stream",
				"worker", consumer, "stream", streamKey, "err", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		var ackIDs []string
		for _, stream := range messages {
			for _, msg := range stream.Messages {
				select {
				case <-ctx.Done():
					return
				default:
				}

				job, ok := q.parseJobMessage(msg, consumer, streamKey)
				if !ok {
					// Unparseable — ACK to prevent infinite redelivery.
					ackIDs = append(ackIDs, msg.ID)
					continue
				}

				if err := q.executeHandler(*job, consumer); err != nil {
					q.logError("queue: job failed",
						"worker", consumer, "job_id", job.ID, "type", job.Type, "err", err.Error())
					q.retryOrDLQ(ctx, *job, streamKey, cfg)
				}
				ackIDs = append(ackIDs, msg.ID)
			}
		}

		q.ack(ctx, streamKey, group, jobType, consumer, ackIDs)
	}
}

// delayedDispatchLoop periodically scans the delayed stream and moves jobs
// whose run_at timestamp has been reached into their target streams.
func (q *Queue) delayedDispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(q.cfg.DelayCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			q.logInfo("queue: delayed dispatcher shutting down")
			return
		case <-ticker.C:
			q.processDelayedJobs(ctx)
		}
	}
}

// processDelayedJobs reads the delayed stream in batches and dispatches
// jobs that are due for execution.
func (q *Queue) processDelayedJobs(ctx context.Context) {
	const batchSize = 100
	delayedKey := q.redis.BuildKey(q.cfg.DelayedStreamKey)
	now := time.Now().Unix()
	horizon := time.Now().Add(q.cfg.DelayCheckInterval + 10*time.Second).Unix()
	startID := "-"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := q.redis.XRangeN(ctx, delayedKey, startID, "+", batchSize)
		if err != nil && err != goredis.Nil {
			if ctx.Err() != nil {
				return
			}
			q.logError("queue: error reading delayed stream", "err", err.Error())
			return
		}
		if len(messages) == 0 {
			return
		}

		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return
			default:
			}

			jobVal, hasJob := msg.Values["job"]
			runAtVal, hasRunAt := msg.Values["run_at"]
			if !hasJob || !hasRunAt {
				q.redis.XDel(ctx, delayedKey, msg.ID) //nolint:errcheck
				continue
			}

			runAt, err := strconv.ParseInt(fmt.Sprintf("%v", runAtVal), 10, 64)
			if err != nil {
				q.redis.XDel(ctx, delayedKey, msg.ID) //nolint:errcheck
				continue
			}

			// Skip jobs that won't be due within the next check horizon.
			if runAt > horizon {
				continue
			}

			// Move to target stream when the job is due.
			if now >= runAt {
				jobStr := fmt.Sprintf("%v", jobVal)
				var job Job
				targetStream := q.redis.BuildKey(fmt.Sprintf("%s:default", q.cfg.StreamPrefix))
				maxLen := q.cfg.DefaultMaxLen

				if err := json.Unmarshal([]byte(jobStr), &job); err == nil {
					cfg := q.resolveType(job.Type)
					targetStream = q.redis.BuildKey(cfg.StreamKey)
					maxLen = cfg.MaxLen
				}

				if _, err := q.redis.XAdd(ctx, &goredis.XAddArgs{
					Stream: targetStream,
					MaxLen: maxLen,
					Approx: true,
					Values: map[string]interface{}{"job": jobStr},
				}); err != nil {
					q.logError("queue: failed to dispatch delayed job", "err", err.Error())
				}
				q.redis.XDel(ctx, delayedKey, msg.ID) //nolint:errcheck
			}
		}

		if int64(len(messages)) < batchSize {
			return
		}
		startID = messages[len(messages)-1].ID
	}
}

// reclaimLoop periodically scans a stream's pending-entry list and re-processes
// jobs that have been idle for longer than Config.ReclaimMinIdle.
func (q *Queue) reclaimLoop(ctx context.Context, streamKey, group, consumer string) {
	ticker := time.NewTicker(q.cfg.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			q.logInfo("queue: reclaimer shutting down",
				"reclaimer", consumer, "stream", streamKey, "group", group)
			return
		case <-ticker.C:
			q.processStuckJobs(ctx, streamKey, group, consumer)
		}
	}
}

// processStuckJobs selects the XAUTOCLAIM (Redis >= 6.2) or XCLAIM path based
// on the server version detected at runtime.
func (q *Queue) processStuckJobs(ctx context.Context, streamKey, group, consumer string) {
	version, err := q.redis.Info(ctx, "server")
	if err == nil {
		if major, minor, ok := parseRedisVersion(version); ok {
			if major > 6 || (major == 6 && minor >= 2) {
				q.processStuckJobsAutoClaim(ctx, streamKey, group, consumer)
				return
			}
		}
	}
	q.processStuckJobsXClaim(ctx, streamKey, group, consumer)
}

// processStuckJobsAutoClaim uses XAUTOCLAIM (Redis >= 6.2).
func (q *Queue) processStuckJobsAutoClaim(ctx context.Context, streamKey, group, consumer string) {
	claimed, _, err := q.redis.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    group,
		Consumer: consumer,
		MinIdle:  q.cfg.ReclaimMinIdle,
		Start:    "0-0",
		Count:    q.cfg.ReclaimBatchSize,
	})
	if err != nil && err != goredis.Nil {
		q.logError("queue: XAUTOCLAIM failed",
			"reclaimer", consumer, "stream", streamKey, "group", group, "err", err.Error())
		return
	}

	q.handleReclaimedMessages(ctx, claimed, streamKey, group, consumer)
}

// processStuckJobsXClaim uses XPENDING + XCLAIM for Redis < 6.2.
func (q *Queue) processStuckJobsXClaim(ctx context.Context, streamKey, group, consumer string) {
	pending, err := q.redis.XPendingExt(ctx, streamKey, group, "-", "+", q.cfg.ReclaimBatchSize, 0)
	if err != nil && err != goredis.Nil {
		q.logError("queue: XPENDINGEXT failed",
			"reclaimer", consumer, "stream", streamKey, "group", group, "err", err.Error())
		return
	}

	var eligibleIDs []string
	for _, entry := range pending {
		if entry.Idle >= q.cfg.ReclaimMinIdle {
			eligibleIDs = append(eligibleIDs, entry.ID)
			q.logInfo("queue: reclaiming stuck job",
				"reclaimer", consumer, "stream", streamKey,
				"job_id", entry.ID, "idle", entry.Idle)
		}
	}
	if len(eligibleIDs) == 0 {
		return
	}

	claimed, err := q.redis.XClaim(ctx, &goredis.XClaimArgs{
		Stream:   streamKey,
		Group:    group,
		Consumer: consumer,
		MinIdle:  q.cfg.ReclaimMinIdle,
		Messages: eligibleIDs,
	})
	if err != nil {
		q.logError("queue: XCLAIM failed",
			"reclaimer", consumer, "stream", streamKey, "group", group, "err", err.Error())
		return
	}

	q.handleReclaimedMessages(ctx, claimed, streamKey, group, consumer)
}

// handleReclaimedMessages re-dispatches a slice of reclaimed messages.
func (q *Queue) handleReclaimedMessages(ctx context.Context, messages []goredis.XMessage, streamKey, group, consumer string) {
	var ackIDs []string

	for _, msg := range messages {
		job, ok := q.parseJobMessage(msg, consumer, streamKey)
		if !ok {
			ackIDs = append(ackIDs, msg.ID)
			continue
		}

		cfg := q.resolveType(job.Type)

		if err := q.executeHandler(*job, consumer); err != nil {
			q.logError("queue: reclaimed job failed",
				"reclaimer", consumer, "job_id", job.ID, "type", job.Type, "err", err.Error())
			q.retryOrDLQ(ctx, *job, streamKey, cfg)
		}
		ackIDs = append(ackIDs, msg.ID)
	}

	q.ack(ctx, streamKey, group, "reclaim", consumer, ackIDs)
}

// dlqCleanupLoop periodically removes DLQ entries that have exceeded
// Config.DLQRetention.
func (q *Queue) dlqCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(q.cfg.DLQCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			q.logInfo("queue: DLQ cleanup shutting down")
			return
		case <-ticker.C:
			q.cleanupDLQ(ctx)
		}
	}
}

// cleanupDLQ scans the DLQ in batches and deletes entries older than DLQRetention.
func (q *Queue) cleanupDLQ(ctx context.Context) {
	const batchSize = 200
	dlqKey := q.redis.BuildKey(q.cfg.DLQStreamKey)
	startID := "-"
	now := time.Now()

	for {
		messages, err := q.redis.XRangeN(ctx, dlqKey, startID, "+", batchSize)
		if err != nil {
			q.logError("queue: error reading DLQ", "err", err.Error())
			return
		}
		if len(messages) == 0 {
			return
		}

		var toDelete []string
		for _, msg := range messages {
			var job Job
			jobStr := fmt.Sprintf("%v", msg.Values["job"])
			if err := json.Unmarshal([]byte(jobStr), &job); err != nil {
				// Malformed entry — remove it.
				toDelete = append(toDelete, msg.ID)
				continue
			}
			if now.Sub(job.CreatedAt) > q.cfg.DLQRetention {
				toDelete = append(toDelete, msg.ID)
				q.logInfo("queue: removing expired DLQ entry",
					"job_id", job.ID, "created_at", job.CreatedAt.Format(time.RFC3339))
			}
		}

		if len(toDelete) > 0 {
			if _, err := q.redis.XDel(ctx, dlqKey, toDelete...); err != nil {
				q.logError("queue: failed to delete DLQ entries", "err", err.Error())
			}
		}

		if len(messages) < batchSize {
			return
		}
		startID = messages[len(messages)-1].ID
	}
}

// ---- helpers ----

// parseJobMessage extracts and unmarshals a Job from an XMessage.
// Returns (nil, false) on any error; logs the problem and caller should ACK.
func (q *Queue) parseJobMessage(msg goredis.XMessage, consumer, streamKey string) (*Job, bool) {
	val, ok := msg.Values["job"]
	if !ok {
		q.logError("queue: message missing 'job' field",
			"consumer", consumer, "stream", streamKey, "msg_id", msg.ID)
		return nil, false
	}

	var job Job
	if err := json.Unmarshal([]byte(fmt.Sprintf("%v", val)), &job); err != nil {
		q.logError("queue: failed to unmarshal job",
			"consumer", consumer, "stream", streamKey, "msg_id", msg.ID, "err", err.Error())
		return nil, false
	}
	return &job, true
}

// parseRedisVersion extracts the major and minor version numbers from the
// raw INFO server output. Returns (0, 0, false) on parse failure.
func parseRedisVersion(infoOutput string) (major, minor int, ok bool) {
	for _, line := range strings.Split(infoOutput, "\n") {
		if !strings.HasPrefix(line, "redis_version:") {
			continue
		}
		ver := strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		parts := strings.Split(ver, ".")
		if len(parts) < 2 {
			return 0, 0, false
		}
		maj, err1 := strconv.Atoi(parts[0])
		min, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			return 0, 0, false
		}
		return maj, min, true
	}
	return 0, 0, false
}
