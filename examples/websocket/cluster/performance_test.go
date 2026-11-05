//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
	"github.com/thanhbvha/go-common/v2/internal/perftest"
)

func TestPerformanceWebSocket(t *testing.T) {
	output := perftest.Require(t)
	redis := os.Getenv("PERF_REDIS_ADDR")
	if redis == "" {
		t.Fatal("dedicated Redis required")
	}
	t.Setenv("WS_EXAMPLE_CLIENT_TOKEN", "performance-token")
	address := freeAddress(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 3)
	type runningRole struct {
		role string
		bus  bus.Bus
	}
	ready := make(chan runningRole, 3)
	for _, role := range []string{"worker", "router", "gateway"} {
		go func() {
			done <- runWithBus(ctx, options{role: role, broker: "redis", redis: redis, addr: address, namespace: "performance-" + filepath.Base(output)}, func(b bus.Bus) { ready <- runningRole{role, b} })
		}()
	}
	defer func() {
		cancel()
		for range 3 {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("gateway shutdown timeout")
			}
		}
	}()
	roles := make(map[string]bus.Bus)
	startup := time.NewTimer(20 * time.Second)
	defer startup.Stop()
	for len(roles) < 3 {
		select {
		case role := <-ready:
			roles[role.role] = role.bus
		case <-startup.C:
			t.Fatal("worker/router/gateway startup timeout")
		}
	}
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(20 * time.Second)
	for {
		response, err := client.Get("http://" + address + "/health")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway readiness timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var results []perftest.Result
	for _, scenario := range []string{"message", "fanout"} {
		cmd := exec.Command(os.Getenv("PERF_WS_LOADGEN"), "-target", "ws://"+address+"/ws", "-scenario", scenario, "-connections", "100", "-group-size", "10", "-accounts", "1", "-messages", "1", "-reconnects", "1", "-event", "business", "-tenant", "demo", "-account", "member")
		drain := func(ctx context.Context) (perftest.Backlog, error) {
			return waitPerformanceDrain(ctx, func(ctx context.Context) ([]transport.TopologyStats, error) {
				task, err := roles["worker"].TopologyStats(ctx, bus.TopologyRef{Kind: bus.TaskTopology, Name: "ws.command"})
				if err != nil {
					return nil, err
				}
				event, err := roles["router"].SubscriberStats(ctx, "ws.notification", "realtime-router")
				if err != nil {
					return nil, err
				}
				return []transport.TopologyStats{task, event}, nil
			}, func(stats []transport.TopologyStats) error {
				return perftest.WriteJSON(filepath.Join(output, "websocket-"+scenario, "broker-stats.json"), stats)
			})
		}
		results = append(results, perftest.ChildWithDrain(t, "websocket-"+scenario, cmd, drain))
	}
	if err := perftest.WriteJSON(filepath.Join(output, "metrics.json"), results); err != nil {
		t.Fatal(err)
	}
}

func waitPerformanceDrain(ctx context.Context, snapshot func(context.Context) ([]transport.TopologyStats, error), save func([]transport.TopologyStats) error) (perftest.Backlog, error) {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		stats, err := snapshot(ctx)
		if err != nil {
			return perftest.Backlog{}, err
		}
		if len(stats) != 2 {
			return perftest.Backlog{}, fmt.Errorf("missing task/subscriber stats")
		}
		var backlog perftest.Backlog
		for _, s := range stats {
			if !s.LagKnown || s.Pending < 0 || s.InFlight < 0 || s.Delayed < 0 || s.DLQ < 0 || s.Lag != s.Pending+s.InFlight {
				return backlog, fmt.Errorf("unknown/invalid broker lag")
			}
			backlog.Pending += s.Pending
			backlog.InFlight += s.InFlight
			backlog.Delayed += s.Delayed
			backlog.DLQ += s.DLQ
		}
		if err := save(stats); err != nil {
			return backlog, err
		}
		if backlog.DLQ != 0 {
			return backlog, fmt.Errorf("unexpected dead letters")
		}
		if backlog == (perftest.Backlog{}) {
			return backlog, nil
		}
		select {
		case <-ctx.Done():
			return backlog, ctx.Err()
		case <-tick.C:
		}
	}
}
