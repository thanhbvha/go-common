// Command foundation runs the gRPC example with foundation-owned signals/lifecycle.
package main

import (
	"context"
	"os"

	"github.com/thanhbvha/go-common/examples/grpc/internal/demo"
)

func main() {
	// foundation.App owns SIGINT/SIGTERM. Do not add another signal.NotifyContext.
	os.Exit(demo.Main(context.Background(), "foundation", os.Args[1:]))
}
