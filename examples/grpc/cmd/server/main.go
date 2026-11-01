// Command server runs the authenticated gRPC demonstration server.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/thanhbvha/go-common/examples/grpc/internal/demo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := demo.Main(ctx, "server", os.Args[1:])
	stop()
	os.Exit(code)
}
