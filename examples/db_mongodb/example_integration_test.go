//go:build integration

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// This executes the same entry point as go run, including state checks and cleanup.
func TestMongoDBExampleAll(t *testing.T) {
	uri := os.Getenv("MONGODB_INTEGRATION_URI")
	if uri == "" {
		t.Skip("set MONGODB_INTEGRATION_URI to an isolated replica set")
	}
	for _, selection := range append([]string{"all"}, exampleNames...) {
		t.Run(selection, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			var output bytes.Buffer
			if err := runExample(ctx, uri, selection, &output); err != nil {
				// Keep connection credentials and server error payloads out of test output.
				t.Fatalf("complete runnable example failed: %v", err)
			}
			for _, name := range exampleNames {
				if selection != "all" && selection != name {
					continue
				}
				if !strings.Contains(output.String(), name+": OK") {
					t.Fatalf("scenario %s did not finish", name)
				}
			}
			if !strings.Contains(output.String(), "cleanup: temporary databases removed and clients closed") {
				t.Fatal("cleanup did not complete")
			}
			t.Log(output.String())
		})
	}
}
