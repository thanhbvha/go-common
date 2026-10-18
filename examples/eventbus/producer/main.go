package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus producer:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (returnErr error) {
	cfg := demo.DefaultConfig()
	fs := flag.NewFlagSet("eventbus-producer", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	mode := fs.String("mode", "ack", "task result scenario: ack, retry, permanent, skip, or slow")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger := demo.NewLogger("producer")
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	var bus eventbus.Bus
	defer func() { returnErr = join(returnErr, demo.CloseAll(bus, rt)) }()
	bus, err = demo.NewBus(cfg, rt, logger, demo.NewMetrics(), demo.NewTracer(logger))
	if err != nil {
		return err
	}
	if err := demo.DeclareTask(bus, cfg.Driver, false, logger); err != nil {
		return err
	}
	if err := demo.DeclareEvent(bus, cfg.Driver); err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}
	payload := demo.UserMessage{UserID: "producer-user", Email: "producer@example.invalid", Mode: *mode}
	if err := payload.Validate(); err != nil {
		return err
	}
	opts := []eventbus.JobOption{eventbus.WithHeader("source", "producer")}
	if demo.SupportsOrdering(rt.Transport) {
		opts = append(opts, eventbus.WithOrderingKey(payload.UserID))
	}
	if demo.SupportsDedupe(rt.Transport) {
		opts = append(opts, eventbus.WithDedupeKey("producer:"+payload.UserID), eventbus.WithDedupeTTL(time.Minute))
	}
	if err := bus.Enqueue(ctx, demo.TaskName, payload, opts...); err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}
	if err := bus.Publish(ctx, demo.EventName, payload, eventbus.WithHeader("source", "producer")); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	logger.Info("messages accepted", "task", demo.TaskName, "event", demo.EventName)
	return nil
}

func join(current, cleanup error) error {
	if current != nil {
		return current
	}
	return cleanup
}
