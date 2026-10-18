package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus worker:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (returnErr error) {
	cfg := demo.DefaultConfig()
	fs := flag.NewFlagSet("eventbus-worker", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger := demo.NewLogger("worker")
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
	if err := demo.DeclareTask(bus, cfg.Driver, true, logger); err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}
	logger.Info("worker ready", "task", demo.TaskName, "namespace", cfg.Namespace)
	<-ctx.Done()
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
