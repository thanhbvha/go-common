// Run from the repository root: go run ./examples/db -list
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

type options struct {
	backend string
	demo    string
	otel    bool
}
type demo struct {
	name, description string
	run               func(context.Context, options) error
}

var demos = []demo{
	{"backend", "configuration, requirements and verified capabilities", demoBackend},
	{"crud", "create, bulk insert, lookup, zero-value patch and soft delete", demoCRUD},
	{"pagination", "filtered pages, typed sorting and both cursor directions", demoPagination},
	{"scope", "tenant policy retained across queries and transactions", demoScope},
	{"upsert", "strict targets, DoNothing and explicit MySQL AnyUniqueKey", demoUpsert},
	{"transactions", "commit, rollback, isolation, read-only and scope guards", demoTransactions},
	{"cas", "optimistic concurrency and stale-version handling", demoCAS},
	{"limits", "batch size, bulk count, payload and pagination limits", demoLimits},
	{"manager", "multiple pools, dynamic Add and wrong-client rejection", demoManager},
	{"admission", "bounded queue, ErrBusy and queued cancellation", demoAdmission},
	{"lifecycle", "bounded shutdown, transaction draining and terminal Close", demoLifecycle},
	{"native", "parameterized native SQL, aggregates and joins", demoNative},
	{"errors", "safe errors.Is/As handling and backend error classification", demoErrors},
	{"telemetry", "instance-owned traces, metrics and Monitor events", demoTelemetry},
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		// run returns sanitized errors. Never print DSNs or unwrapped native errors.
		slog.Error("ORM example failed", "error", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("db-example", flag.ContinueOnError)
	flags.SetOutput(out)
	opt := options{}
	list := false
	flags.StringVar(&opt.backend, "backend", "sqlite", "sqlite, postgres or mysql")
	flags.StringVar(&opt.demo, "demo", "all", "all or comma-separated demo names")
	flags.BoolVar(&opt.otel, "otel", false, "capture local traces/metrics for every demo")
	flags.BoolVar(&list, "list", false, "list demos without connecting to a database")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument")
	}
	if list {
		for _, d := range demos {
			fmt.Fprintf(out, "%-14s %s\n", d.name, d.description)
		}
		return nil
	}
	if opt.backend != "sqlite" && opt.backend != "postgres" && opt.backend != "mysql" {
		return fmt.Errorf("backend must be sqlite, postgres or mysql")
	}
	requested := map[string]bool{}
	if opt.demo != "all" {
		for _, name := range strings.Split(opt.demo, ",") {
			requested[strings.TrimSpace(name)] = true
		}
		for name := range requested {
			found := false
			for _, d := range demos {
				found = found || name == d.name
			}
			if !found {
				return fmt.Errorf("unknown demo: %s (use -list)", name)
			}
		}
	}
	// Demo bodies print only synthetic IDs/counts and safe metadata.
	for _, d := range demos {
		if opt.demo != "all" && !requested[d.name] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		fmt.Fprintf(out, "\n[%s / %s] %s\n", opt.backend, d.name, d.description)
		if err := d.run(ctx, opt); err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
	}
	fmt.Fprintln(out, "\nAll selected demos completed.")
	return nil
}

func require(condition bool, message string) error {
	if !condition {
		return fmt.Errorf("example assertion: %s", message)
	}
	return nil
}
func expect(err, target error) error {
	if !errors.Is(err, target) {
		return fmt.Errorf("example expected %v", target)
	}
	return nil
}
func nativeFailure(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("native example operation failed: %s", operation)
}
func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
