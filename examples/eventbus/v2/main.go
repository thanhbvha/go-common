// Command v2 demonstrates the public EventBus v2 API without an external broker.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
)

type message struct {
	Account string `json:"account"`
	Mode    string `json:"mode"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus v2 example:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (result error) {
	cfg := eventbus.DefaultConfig()
	cfg.Namespace = "example_eventbus_v2_api"
	cfg.Concurrency, cfg.Prefetch = 2, 2
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(memory.New()))
	if err != nil {
		return err
	}
	defer func() {
		// Use a fresh deadline even when startup or processing timed out.
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		result = errors.Join(result, bus.Close(closeCtx))
	}()

	const task, event = "report.generate", "report.requested"
	var repaired atomic.Bool
	var taskCalls, eventCalls atomic.Int32
	if err := bus.DeclareTask(task, eventbus.WithMaxAttempts(3),
		eventbus.WithConcurrencyKeyLimit(1), eventbus.WithMaxPayloadBytes(4096)); err != nil {
		return err
	}
	if err := bus.HandleTask(task, eventbus.Handle(func(ctx context.Context, job eventbus.Job, payload message) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if payload.Account == "" {
			return eventbus.MarkPermanent(errors.New("account required"))
		}
		taskCalls.Add(1)
		switch payload.Mode {
		case "retry":
			if job.Attempt() == 1 {
				return eventbus.MarkRetryAfter(errors.New("temporary dependency failure"), 20*time.Millisecond)
			}
		case "permanent":
			if !repaired.Load() {
				return eventbus.MarkPermanent(errors.New("synthetic dependency configuration failure"))
			}
		case "skip":
			return eventbus.MarkSkip("already processed")
		}
		// Real side effects must use a durable idempotency record keyed by job.ID()
		// or a business operation ID. Publication dedupe alone is insufficient.
		return nil
	})); err != nil {
		return err
	}
	if err := bus.DeclareEvent(event); err != nil {
		return err
	}
	for _, group := range []string{"audit", "notification"} {
		if err := bus.Subscribe(event, group, eventbus.Handle(func(_ context.Context, _ eventbus.Job, payload message) error {
			if payload.Account == "" {
				return eventbus.MarkPermanent(errors.New("account required"))
			}
			eventCalls.Add(1)
			return nil
		}), eventbus.WithConcurrency(1)); err != nil {
			return err
		}
	}
	startCtx, cancel := context.WithTimeout(ctx, time.Second)
	err = bus.Start(startCtx)
	cancel() // Start's context controls provisioning, not consumer lifetime.
	if err != nil {
		return err
	}
	if err := bus.Ping(ctx); err != nil {
		return err
	}

	first, err := bus.EnqueueWithResult(ctx, task, message{Account: "demo-account"},
		eventbus.WithDedupeKey("report:demo:once"), eventbus.WithDedupeTTL(time.Minute),
		eventbus.WithConcurrencyKey("demo-account"), eventbus.WithHeader("source", "v2-example"))
	if err != nil {
		return err
	}
	duplicate, err := bus.EnqueueWithResult(ctx, task, message{Account: "demo-account"},
		eventbus.WithDedupeKey("report:demo:once"), eventbus.WithDedupeTTL(time.Minute))
	if err != nil {
		return err
	}
	if !duplicate.Duplicate || first.ID != duplicate.ID {
		return errors.New("publication identity was not preserved")
	}
	batch, err := bus.EnqueueBatchWithResult(ctx, task, []eventbus.BatchJob{
		{Payload: message{Account: "demo-account", Mode: "retry"}},
		{Payload: message{Account: "demo-account", Mode: "skip"}},
		{Payload: message{Account: "demo-account", Mode: "permanent"}},
		{Payload: message{Account: "demo-account"}, Options: []eventbus.JobOption{eventbus.WithDelay(30 * time.Millisecond)}},
		{Payload: message{Account: "demo-account"}, Options: []eventbus.JobOption{eventbus.WithRunAt(time.Now().Add(40 * time.Millisecond))}},
	})
	if err != nil {
		return fmt.Errorf("batch accepted %d items: %w", len(batch.Results), err)
	}
	if _, err := bus.PublishWithResult(ctx, event, message{Account: "demo-account"}); err != nil {
		return err
	}
	if err := wait(ctx, func() (bool, error) {
		stats, err := bus.TopologyStats(ctx, eventbus.TaskRef(task))
		return stats.Pending == 0 && stats.InFlight == 0 && stats.Delayed == 0 && stats.DLQ == 1 && eventCalls.Load() == 2, err
	}); err != nil {
		return err
	}

	if err := bus.Pause(ctx, task); err != nil {
		return err
	}
	if err := bus.DrainTopology(ctx, task); err != nil {
		return err
	}
	page, err := bus.ListDeadLetters(ctx, eventbus.TaskRef(task), "", 10)
	if err != nil {
		return err
	}
	if len(page.Records) != 1 {
		return errors.New("expected one dead letter")
	}
	repaired.Store(true) // Correct the cause before requeueing.
	if err := bus.RequeueDeadLetters(ctx, eventbus.TaskRef(task), []string{page.Records[0].ID}); err != nil {
		return err
	}
	if err := bus.Resume(ctx, task); err != nil {
		return err
	}
	if err := wait(ctx, func() (bool, error) {
		stats, err := bus.TopologyStats(ctx, eventbus.TaskRef(task))
		return stats.Pending == 0 && stats.InFlight == 0 && stats.Delayed == 0 && stats.DLQ == 0, err
	}); err != nil {
		return err
	}
	for _, group := range []string{"audit", "notification"} {
		if err := wait(ctx, func() (bool, error) {
			stats, err := bus.SubscriberStats(ctx, event, group)
			return stats.Pending == 0 && stats.InFlight == 0, err
		}); err != nil {
			return err
		}
	}
	fmt.Printf("v2 complete: batch=%d duplicate=%t task_calls=%d event_groups=%d dlq=0\n", len(batch.Results), duplicate.Duplicate, taskCalls.Load(), eventCalls.Load())
	return nil
}

func wait(ctx context.Context, ready func() (bool, error)) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := ready()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
