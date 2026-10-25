package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
	commontelemetry "github.com/thanhbvha/go-common/telemetry"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus observability example:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (returnErr error) {
	cfg, err := demo.LoadConfig(args)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("eventbus-observability", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	otelEndpoint := fs.String("otel-endpoint", "127.0.0.1:4317", "OTLP gRPC collector endpoint")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	telCfg := commontelemetry.DefaultConfig("eventbus-example")
	telCfg.Endpoint = *otelEndpoint
	telCfg.ExportInterval = 2 * time.Second
	tel, err := commontelemetry.Init(ctx, telCfg)
	if err != nil {
		return fmt.Errorf("initialize OpenTelemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		returnErr = errors.Join(returnErr, tel.Shutdown(shutdownCtx))
	}()

	metrics, err := demo.NewOTelMetrics()
	if err != nil {
		return fmt.Errorf("create EventBus metrics: %w", err)
	}
	logger := demo.NewLogger("observability")
	defer logger.Close()
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	var bus eventbus.Bus
	defer func() { returnErr = errors.Join(returnErr, demo.CloseAll(bus, rt)) }()

	bus, err = demo.NewBus(cfg, rt, logger, metrics, demo.NewOTelTracer())
	if err != nil {
		return err
	}
	topologyOptions := append(demo.TopologyOptions(cfg.Driver),
		eventbus.WithRateLimit(25, 5),
		eventbus.WithOrderingKeyConcurrency(1),
		eventbus.WithMaxPayloadBytes(64*1024),
	)
	if err := bus.DeclareTask(demo.TaskName, topologyOptions...); err != nil {
		return err
	}
	completed := make(chan struct{}, 2)
	handler := demo.TaskHandler(logger)
	if err := bus.HandleTask(demo.TaskName, func(ctx context.Context, job eventbus.Job) error {
		err := handler(ctx, job)
		if err == nil {
			completed <- struct{}{}
		}
		return err
	}); err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}

	if err := bus.Pause(ctx, demo.TaskName); err != nil {
		return fmt.Errorf("pause task: %w", err)
	}
	batch := []eventbus.BatchJob{
		{Payload: demo.UserMessage{UserID: "otel-1", Email: "otel-1@example.invalid"}},
		{Payload: demo.UserMessage{UserID: "otel-2", Email: "otel-2@example.invalid"}},
	}
	if err := bus.EnqueueBatch(ctx, demo.TaskName, batch); err != nil {
		return fmt.Errorf("enqueue batch: %w", err)
	}
	if err := bus.DrainTopology(ctx, demo.TaskName); err != nil {
		return fmt.Errorf("drain paused task: %w", err)
	}
	if err := bus.Resume(ctx, demo.TaskName); err != nil {
		return fmt.Errorf("resume task: %w", err)
	}

	for range batch {
		select {
		case <-completed:
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timed out waiting for task completion")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	stats, err := bus.Stats(ctx, demo.TaskName)
	if err != nil {
		return fmt.Errorf("read task stats: %w", err)
	}
	logger.Info("operational sample complete", "pending", stats.Pending, "in_flight", stats.InFlight, "dlq", stats.DLQ)
	return nil
}
