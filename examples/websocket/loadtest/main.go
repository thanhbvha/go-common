package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/thanhbvha/go-common/examples/websocket/internal/exampleconfig"
)

func main() {
	path := exampleconfig.Path(os.Args[1:])
	cfg, err := exampleconfig.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot load example configuration")
		os.Exit(1)
	}
	var o options
	flag.String("config", path, "YAML or .env configuration file")
	flag.StringVar(&o.target, "target", cfg.LoadTarget, "WebSocket URL")
	flag.StringVar(&o.token, "token", cfg.ClientToken, "Bearer token (prefer environment)")
	flag.StringVar(&o.tenant, "tenant", cfg.ClientTenant, "Tenant ID")
	flag.StringVar(&o.account, "account", cfg.ClientAccount, "Account ID; suffixed when accounts > 1")
	flag.IntVar(&o.accounts, "accounts", cfg.LoadAccounts, "Demo accounts, clients assigned round-robin")
	flag.IntVar(&o.connections, "connections", cfg.LoadConnections, "Concurrent connections")
	flag.IntVar(&o.messages, "messages", cfg.LoadMessages, "Messages per connection per cycle")
	flag.DurationVar(&o.hold, "hold", cfg.LoadHold, "Idle hold with ping/pong reader running")
	flag.IntVar(&o.slowReaders, "slow-readers", cfg.LoadSlowReaders, "Percentage delaying application reads by 100ms")
	flag.StringVar(&o.event, "event", cfg.LoadEvent, "Request/reply event for message/reconnect")
	flag.StringVar(&o.scenario, "scenario", cfg.LoadScenario, "message, idle, churn, fanout, reconnect, rolling-drain")
	flag.IntVar(&o.reconnects, "reconnects", cfg.LoadReconnects, "Connection cycles for reconnect/churn")
	flag.DurationVar(&o.churnDelay, "churn-delay", cfg.LoadChurnDelay, "Delay between cycles")
	flag.IntVar(&o.groupSize, "group-size", cfg.LoadGroupSize, "Fanout recipients per room, including sender")
	flag.Float64Var(&o.rate, "rate", cfg.LoadRate, "Messages/second/connection; 0 means unpaced")
	output := flag.String("output", cfg.LoadOutput, "Optional JSON result path")
	flag.Parse()
	if err := o.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	startGoroutines := runtime.NumGoroutine()
	metrics := run(o)
	runtime.GC()
	runtime.ReadMemStats(&after)
	metrics.ClientGoroutinesBefore, metrics.ClientGoroutinesAfter = startGoroutines, runtime.NumGoroutine()
	metrics.ClientHeapBefore, metrics.ClientHeapAfter = before.HeapAlloc, after.HeapAlloc
	encoded, err := json.MarshalIndent(metrics, "", "  ")
	if err == nil && *output != "" {
		err = os.WriteFile(*output, encoded, 0o600)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot write load result")
		os.Exit(1)
	}
	fmt.Println(string(encoded))
	if metrics.ConnectErrors > 0 || metrics.MessageErrors > 0 || metrics.Missing > 0 || metrics.Duplicates > 0 {
		os.Exit(1)
	}
}

func percentile(values []time.Duration, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return float64(values[int(float64(len(values)-1)*fraction)]) / float64(time.Millisecond)
}
