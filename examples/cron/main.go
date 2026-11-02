package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/cache"
	"github.com/thanhbvha/go-common/cron"
)

func main() {
	mode := flag.String("mode", "local", "example mode: local or distributed")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis address for distributed mode")
	namespace := flag.String("namespace", "reports:production", "Redis namespace for distributed mode")
	flag.Parse()
	switch *mode {
	case "local":
		RunStandardCronExample()
	case "distributed":
		RunDistributedCronExample(*redisAddr, *namespace)
	default:
		log.Fatalf("unknown mode %q: use local or distributed", *mode)
	}
}

func RunStandardCronExample() {
	scheduler := cron.NewScheduler(cron.WithLocation(time.UTC))
	_, err := scheduler.AddJob(cron.JobConfig{
		JobName: "local-metrics", Schedule: "*/5 * * * * *", Timeout: 3 * time.Second,
		Run: func(ctx context.Context) error { slog.InfoContext(ctx, "local metrics collected"); return nil },
	})
	if err != nil {
		log.Fatal(err)
	}
	scheduler.Start()
	waitForInterrupt(scheduler)
}

func RunDistributedCronExample(redisAddr, namespace string) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr, ContextTimeoutEnabled: true,
		DialTimeout: 2 * time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	// Resources stay available until scheduler shutdown has finished.
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := rdb.Ping(ctx).Err()
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	scheduler := cron.NewScheduler(
		cron.WithLocation(time.UTC), cron.WithNamespace(namespace),
		cron.WithObserver(func(result cron.Result) {
			// Export counters/histograms here. This hook may run concurrently.
			// A canceled invocation during shutdown is expected, not a failed report.
			if result.Err != nil && result.Status != "canceled" {
				slog.Error("report execution failed", "status", result.Status, "error", result.Err)
			}
		}),
	)
	_, err = scheduler.AddDistributedJob(cron.DistributedConfig{
		JobName: "report-generator", Schedule: "*/10 * * * * *",
		LockTTL: 15 * time.Second, LockTimeout: 2 * time.Second,
		DeduplicationTTL: 48 * time.Hour, Timeout: time.Minute,
		Run: func(ctx context.Context) error {
			execution, ok := cron.ExecutionFromContext(ctx)
			if !ok {
				return fmt.Errorf("missing cron execution metadata")
			}
			idempotencyKey := execution.JobName + ":" + execution.ScheduledAt.UTC().Format(time.RFC3339Nano)
			// Enforce this key with a unique constraint/transaction in the real write.
			slog.InfoContext(ctx, "generating report", "idempotency_key", idempotencyKey)
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				fmt.Println("Report generated")
				return nil
			}
		},
	}, cache.NewRedisLock(rdb))
	if err != nil {
		log.Fatal(err)
	}
	scheduler.Start()
	waitForInterrupt(scheduler)
}

func waitForInterrupt(scheduler *cron.Scheduler) {
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-signalCtx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := scheduler.Shutdown(ctx); err != nil {
		// Avoid returning to main and closing dependencies while callbacks still run.
		// A supervisor can enforce an outer process termination deadline.
		slog.Error("scheduler shutdown exceeded grace period; waiting for callbacks", "error", err)
		_ = scheduler.Shutdown(context.Background())
	}
}
