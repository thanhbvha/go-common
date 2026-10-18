package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
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
	topo := transport.Topology{Namespace: cfg.Namespace, JobType: demo.TaskName, Kind: eventbus.TaskTopology}
	switch *action {
	case "stats":
		return printStats(ctx, rt.Transport, topo)
	case "dlq-count":
		admin, err := dlqAdmin(rt.Transport)
		if err != nil {
			return err
		}
		count, err := admin.Count(ctx, topo)
		if err != nil {
			return err
		}
		fmt.Printf("dlq_count=%d\n", count)
		return nil
	case "dlq-list":
		return listDLQ(ctx, rt.Transport, topo, *cursor, *limit)
	case "dlq-requeue":
		selected := demo.SplitCSV(*ids)
		if len(selected) == 0 {
			return fmt.Errorf("dlq-requeue requires --ids with DeadLetter.ID values")
		}
		admin, err := dlqAdmin(rt.Transport)
		if err != nil {
			return err
		}
		if err := admin.Requeue(ctx, topo, selected); err != nil {
			return err
		}
		fmt.Printf("requeued=%d\n", len(selected))
		return nil
	case "dlq-purge":
		if !*confirmPurge {
			return fmt.Errorf("dlq-purge requires --confirm-purge")
		}
		admin, err := dlqAdmin(rt.Transport)
		if err != nil {
			return err
		}
		if err := admin.Purge(ctx, topo); err != nil {
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

func printStats(ctx context.Context, tr transport.Transport, topo transport.Topology) error {
	reader, ok := tr.(transport.StatsReader)
	if !ok {
		return fmt.Errorf("transport %s does not support stats", tr.Name())
	}
	stats, err := reader.Stats(ctx, topo)
	if err != nil {
		return err
	}
	fmt.Printf("pending=%d delayed=%d in_flight=%d dlq=%d lag=%d lag_known=%t oldest_age=%s oldest_age_known=%t\n",
		stats.Pending, stats.Delayed, stats.InFlight, stats.DLQ, stats.Lag, stats.LagKnown, stats.OldestAge, stats.OldestAgeKnown)
	return nil
}

func dlqAdmin(tr transport.Transport) (transport.DLQAdmin, error) {
	admin, ok := tr.(transport.DLQAdmin)
	if !ok {
		return nil, fmt.Errorf("transport %s does not support DLQ administration", tr.Name())
	}
	return admin, nil
}

func listDLQ(ctx context.Context, tr transport.Transport, topo transport.Topology, cursor string, limit int) error {
	var records []transport.DeadLetter
	if cursorAdmin, ok := tr.(transport.CursorDLQAdmin); ok {
		page, err := cursorAdmin.ListPage(ctx, topo, cursor, limit)
		if err != nil {
			return err
		}
		records = page.Records
		fmt.Printf("next_cursor=%q\n", page.NextCursor)
	} else {
		if cursor != "" {
			return fmt.Errorf("transport %s does not support cursor pagination", tr.Name())
		}
		admin, err := dlqAdmin(tr)
		if err != nil {
			return err
		}
		records, err = admin.List(ctx, topo, 0, limit)
		if err != nil {
			return err
		}
	}
	for _, record := range records {
		fmt.Printf("id=%q reason=%q attempts=%d failed_at=%q source=%q source_group=%q source_position=%q\n",
			record.ID, record.Reason, record.Attempts, record.FailedAt, record.Source, record.SourceGroup, record.SourcePosition)
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
