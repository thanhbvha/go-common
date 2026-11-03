// Command certgen creates development-only TLS/mTLS certificates for localhost.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/thanhbvha/go-common/v2/examples/grpc/internal/devcert"
)

func main() {
	directory := flag.String("out", "examples/grpc/certs", "new output directory; parent must exist")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(1)
	}
	if err := devcert.Generate(*directory); err != nil {
		fmt.Fprintln(os.Stderr, "Certificate generation failed; ensure output directory is new and writable.")
		os.Exit(1)
	}
	fmt.Println("Created localhost CA, server and client certificates (valid 24h). Development only; protect the private key files.")
}
