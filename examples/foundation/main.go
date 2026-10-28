package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

func main() {
	demo := flag.String("demo", "api", "demo to run: api, fiber, worker, or full")
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
	default:
		err = fmt.Errorf("unknown demo %q (use api, fiber, worker, or full)", *demo)
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
