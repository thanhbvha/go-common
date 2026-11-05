// Command loadgen drives the reference HTTP server from a separate process.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/thanhbvha/go-common/v2/internal/perftest"
)

func main() {
	target := flag.String("target", "", "fixture URL")
	workers := flag.Int("concurrency", 1, "concurrent requests")
	flag.Parse()
	if *workers < 1 || *workers > 256 || *target == "" {
		fmt.Fprintln(os.Stderr, "target and concurrency 1..256 required")
		os.Exit(1)
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: *workers, MaxIdleConnsPerHost: *workers, MaxConnsPerHost: *workers}}
	defer client.CloseIdleConnections()
	operations := make([]func(context.Context) error, *workers)
	for i := range operations {
		operations[i] = func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, "GET", *target, nil)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+os.Getenv("PERF_HTTP_TOKEN"))
			response, err := client.Do(req)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			var data struct {
				UserID string `json:"user_id"`
			}
			if response.StatusCode != 200 {
				io.Copy(io.Discard, response.Body)
				return fmt.Errorf("unexpected status %d", response.StatusCode)
			}
			if err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&data); err != nil {
				return err
			}
			if data.UserID != "performance" {
				return fmt.Errorf("incorrect authenticated response")
			}
			return nil
		}
	}
	result, err := perftest.Load(fmt.Sprintf("http-%d", *workers), operations)
	if err == nil {
		err = perftest.WriteJSON(filepath.Join(os.Getenv("PERF_PHASE_DIR"), "client.json"), result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
