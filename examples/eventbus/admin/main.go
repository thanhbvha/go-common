package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/examples/eventbus/internal/demo"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "eventbus admin:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (returnErr error) {
	cfg := demo.DefaultConfig()
	fs := flag.NewFlagSet("eventbus-admin", flag.ContinueOnError)
	demo.BindFlags(fs, &cfg)
	action := fs.String("action", "stats", "stats, dlq-count, dlq-list, dlq-requeue, or dlq-purge")
	ids := fs.String("ids", "", "comma-separated DeadLetter.ID values for dlq-requeue")
	cursor := fs.String("cursor", "", "opaque cursor for dlq-list when supported")
	limit := fs.Int("limit", 20, "maximum DLQ records to list")
	confirmPurge := fs.Bool("confirm-purge", false, "confirm destructive logical DLQ purge")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit < 1 || *limit > 1000 {
		return fmt.Errorf("limit must be between 1 and 1000")
	}
	logger := demo.NewLogger("admin")
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() { returnErr = join(returnErr, demo.CloseAll(nil, rt)) }()
	if err := rt.Transport.Ping(ctx); err != nil {
		return fmt.Errorf("ping transport: %w", err)
	}
	bus, err := eventbus.Open(eventbus.Config{Namespace: cfg.Namespace}, eventbus.WithTransport(rt.Transport))
	if err != nil {
		return err
	}
	if err := bus.DeclareTask(demo.TaskName, demo.TopologyOptions(cfg.Driver)...); err != nil {
		return err
	}
	ref := eventbus.TaskRef(demo.TaskName)
	switch *action {
	case "stats":
		stats, err := bus.TopologyStats(ctx, ref)
		if err != nil {
			return err
		}
		fmt.Printf("pending=%d delayed=%d in_flight=%d dlq=%d lag=%d lag_known=%t oldest_age=%s oldest_age_known=%t\n",
			stats.Pending, stats.Delayed, stats.InFlight, stats.DLQ, stats.Lag, stats.LagKnown, stats.OldestAge, stats.OldestAgeKnown)
		return nil
	case "dlq-count":
		count, err := bus.CountDeadLetters(ctx, ref)
		if err != nil {
			return err
		}
		fmt.Printf("dlq_count=%d\n", count)
		return nil
	case "dlq-list":
		page, err := bus.ListDeadLetters(ctx, ref, *cursor, *limit)
		if err != nil {
			return err
		}
		fmt.Printf("next_cursor=%q\n", page.NextCursor)
		for _, record := range page.Records {
			fmt.Printf("id=%q reason=%q attempts=%d failed_at=%q source=%q source_group=%q source_position=%q\n",
				record.ID, record.Reason, record.Attempts, record.FailedAt, record.Source, record.SourceGroup, record.SourcePosition)
		}
		return nil
	case "dlq-requeue":
		selected := demo.SplitCSV(*ids)
		if len(selected) == 0 {
			return fmt.Errorf("dlq-requeue requires --ids with DeadLetter.ID values")
		}
		if err := bus.RequeueDeadLetters(ctx, ref, selected); err != nil {
			return err
		}
		fmt.Printf("requeued=%d\n", len(selected))
		return nil
	case "dlq-purge":
		if !*confirmPurge {
			return fmt.Errorf("dlq-purge requires --confirm-purge")
		}
		if err := bus.PurgeDeadLetters(ctx, ref); err != nil {
			return err
		}
		fmt.Println("dlq_purged=true")
		if cfg.Driver == "kafka" {
			fmt.Println("note=Kafka purge advances the logical admin cursor; physical records remain until retention")
		}
		return nil
	default:
		return fmt.Errorf("unsupported action %q", strings.TrimSpace(*action))
	}
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
