package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
	"github.com/thanhbvha/go-common/utils/graceful"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus subscriber:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := demo.LoadConfig(args)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("eventbus-subscriber", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	group := fs.String("group", "audit", "durable subscriber group")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*group) == "" || *group != strings.TrimSpace(*group) {
		return fmt.Errorf("group must not be blank or have surrounding whitespace")
	}
	logger := demo.NewLogger("subscriber")
	shutdown := graceful.NewShutdown(15 * time.Second)
	shutdown.Register(func(context.Context) error { logger.Close(); return nil })
	waited := false
	defer func() {
		if !waited {
			shutdown.Execute()
		}
	}()
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	var bus eventbus.Bus
	shutdown.Register(func(context.Context) error { return demo.CloseAll(bus, rt) })
	bus, err = demo.NewBus(cfg, rt, logger, demo.NewMetrics(), demo.NewTracer(logger))
	if err != nil {
		return err
	}
	if err := demo.DeclareEvent(bus, cfg.Driver); err != nil {
		return err
	}
	if err := bus.Subscribe(demo.EventName, *group, demo.EventHandler(*group, logger)); err != nil {
		return err
	}
	if err := demo.Start(ctx, bus); err != nil {
		return err
	}
	logger.Info("subscriber ready", "event", demo.EventName, "group", *group, "namespace", cfg.Namespace)
	waited = true
	shutdown.Wait()
	return nil
}
