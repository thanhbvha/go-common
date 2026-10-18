package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus example:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (returnErr error) {
	cfg := demo.DefaultConfig()
	fs := flag.NewFlagSet("eventbus", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	logger := demo.NewLogger("quickstart")
	metrics := demo.NewMetrics()
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	var bus eventbus.Bus
	defer func() {
		returnErr = join(returnErr, demo.CloseAll(bus, rt))
	}()

	bus, err = demo.NewBus(cfg, rt, logger, metrics, demo.NewTracer(logger))
	if err != nil {
		return err
	}
	if err := demo.DeclareTask(bus, cfg.Driver, true, logger); err != nil {
		return err
	}
	if err := demo.DeclareEvent(bus, cfg.Driver); err != nil {
		return err
	}
	if err := bus.Subscribe(demo.EventName, "notification", demo.EventHandler("notification", logger)); err != nil {
		return err
	}
	if err := bus.Subscribe(demo.EventName, "audit", demo.EventHandler("audit", logger)); err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}

	base := demo.UserMessage{UserID: "user-123", Email: "demo@example.invalid"}
	if err := enqueueScenarios(ctx, bus, rt.Transport, base, logger); err != nil {
		return err
	}
	if err := bus.Publish(ctx, demo.EventName, base, eventbus.WithHeader("source", "quickstart")); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	wait := cfg.DemoWait(2 * time.Second)
	logger.Info("waiting for deterministic scenarios", "duration", wait)
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		return ctx.Err()
	}
	stats, err := bus.Stats(ctx, demo.TaskName)
	if err != nil {
		return fmt.Errorf("read stats: %w", err)
	}
	logger.Info("task stats", "pending", stats.Pending, "delayed", stats.Delayed, "in_flight", stats.InFlight, "dlq", stats.DLQ, "lag_known", stats.LagKnown)
	logger.Info("metric counters", "values", metrics.Snapshot())
	return nil
}

func enqueueScenarios(ctx context.Context, bus eventbus.Bus, tr transport.Transport, base demo.UserMessage, logger *demo.Logger) error {
	scenarios := []demo.UserMessage{
		base,
		{UserID: "user-retry", Email: "retry@example.invalid", Mode: "retry"},
		{UserID: "user-permanent", Email: "permanent@example.invalid", Mode: "permanent"},
		{UserID: "user-skip", Email: "skip@example.invalid", Mode: "skip"},
	}
	for _, payload := range scenarios {
		opts := []eventbus.JobOption{eventbus.WithHeader("source", "quickstart")}
		if demo.SupportsOrdering(tr) {
			opts = append(opts, eventbus.WithOrderingKey(payload.UserID))
		}
		if err := bus.Enqueue(ctx, demo.TaskName, payload, opts...); err != nil {
			return fmt.Errorf("enqueue %s: %w", payload.Mode, err)
		}
	}
	if !demo.SupportsOrdering(tr) {
		logger.Info("ordering-key scenario skipped", "reason", "transport does not advertise ordering")
	}
	delayed := demo.UserMessage{UserID: "user-delayed", Email: "delay@example.invalid"}
	if err := bus.Enqueue(ctx, demo.TaskName, delayed, eventbus.WithDelay(2*time.Second)); err != nil {
		return fmt.Errorf("enqueue delayed task: %w", err)
	}
	runAt := demo.UserMessage{UserID: "user-run-at", Email: "run-at@example.invalid"}
	if err := bus.Enqueue(ctx, demo.TaskName, runAt, eventbus.WithRunAt(time.Now().Add(2*time.Second))); err != nil {
		return fmt.Errorf("enqueue run-at task: %w", err)
	}
	if demo.SupportsDedupe(tr) {
		dedupe := demo.UserMessage{UserID: "user-dedupe", Email: "dedupe@example.invalid"}
		for range 2 {
			if err := bus.Enqueue(ctx, demo.TaskName, dedupe, eventbus.WithDedupeKey("welcome:user-dedupe"), eventbus.WithDedupeTTL(time.Minute)); err != nil {
				return fmt.Errorf("enqueue deduplicated task: %w", err)
			}
		}
	} else {
		logger.Info("dedupe scenario skipped", "reason", "transport does not advertise TopologyDeduper")
	}
	return nil
}

func join(current, cleanup error) error {
	if current != nil && cleanup != nil {
		return fmt.Errorf("%v; cleanup: %w", current, cleanup)
	}
	if current != nil {
		return current
	}
	return cleanup
}
