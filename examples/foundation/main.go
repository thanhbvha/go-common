package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/thanhbvha/go-common/v2/foundation"
)

func main() {
	demo := flag.String("demo", "api", "demo to run: api, fiber, worker, full, or lifecycle")
	scenario := flag.String("scenario", "normal", "lifecycle scenario: normal, startup-timeout, stop-timeout, runner-timeout, runtime-failure, missing, cycle")
	address := flag.String("addr", "127.0.0.1:8080", "HTTP address for lifecycle demo")
	stopAfter := flag.Duration("stop-after", 0*time.Second, "cancel lifecycle demo after this duration; zero waits for a signal")
	flag.Parse()

	var err error
	switch *demo {
	case "api":
		err = runAPI(context.Background())
	case "fiber":
		err = runFiberAPI(context.Background())
	case "worker":
		err = runWorker(context.Background())
	case "full":
		err = runFull(context.Background())
	case "lifecycle":
		err = runLifecycle(context.Background(), *scenario, *address, *stopAfter)
	default:
		err = fmt.Errorf("unknown demo %q (use api, fiber, worker, full, or lifecycle)", *demo)
	}
	if err != nil {
		var timeout *foundation.TimeoutError
		if errors.As(err, &timeout) {
			slog.Error("lifecycle timeout", "component", timeout.Component, "phase", timeout.Phase,
				"operation", timeout.Operation, "elapsed", timeout.Elapsed, "budget", timeout.Budget, "pending", timeout.Pending)
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
