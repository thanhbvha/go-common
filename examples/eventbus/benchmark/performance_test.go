//go:build integration

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/eventbus"
	kafkadriver "github.com/thanhbvha/go-common/v2/eventbus/driver/kafka"
	"github.com/thanhbvha/go-common/v2/examples/eventbus/internal/demo"
	"github.com/thanhbvha/go-common/v2/internal/perftest"
)

const performanceTask = "performance_immediate"

type performancePayload struct {
	Sequence uint64 `json:"sequence"`
	Data     string `json:"data"`
}

// Reuse the example's runtime, topology options and quiet observers. The producer
// runs in a separate process; a fixed-size pipe acknowledges handler completion.
func performanceBus(t *testing.T) (eventbus.Bus, string) {
	t.Helper()
	perftest.Require(t)
	var config struct {
		Driver string `json:"driver"`
	}
	if err := json.Unmarshal([]byte(os.Getenv("PERF_CONFIG")), &config); err != nil {
		t.Fatal(err)
	}
	cfg := demo.DefaultConfig()
	cfg.Driver = config.Driver
	cfg.Namespace = os.Getenv("PERF_NAMESPACE")
	cfg.RedisAddr = os.Getenv("EVENTBUS_REDIS_ADDR")
	cfg.NATSURLs = os.Getenv("EVENTBUS_NATS_URLS")
	cfg.KafkaBrokers = os.Getenv("EVENTBUS_KAFKA_BROKERS")
	endpoint := map[string]string{"redis": cfg.RedisAddr, "nats": cfg.NATSURLs, "kafka": cfg.KafkaBrokers}[cfg.Driver]
	if endpoint == "" || cfg.Namespace == "" {
		t.Fatal("owned fixture endpoint/namespace required")
	}
	logger := demo.NewLoggerTo("performance", io.Discard)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rt, err := demo.OpenRuntime(ctx, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	busCfg := eventbus.DefaultConfig()
	busCfg.Namespace = cfg.Namespace
	busCfg.Concurrency = 10
	busCfg.Prefetch = 20
	busCfg.Logger = logger
	busCfg.Metrics = quietMetrics{}
	busCfg.Tracer = quietTracer{}
	bus, err := eventbus.Open(busCfg, eventbus.WithTransport(rt.Transport))
	if err != nil {
		rt.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := demo.CloseAll(bus, rt); err != nil {
			t.Error(err)
		}
	})
	options := append(demo.TopologyOptions(cfg.Driver), eventbus.WithConcurrency(10), eventbus.WithPrefetch(20))
	if cfg.Driver == "kafka" {
		options = append(options, kafkadriver.WithPartitions(10))
	}
	if err := bus.DeclareTask(performanceTask, options...); err != nil {
		t.Fatal(err)
	}
	return bus, cfg.Driver
}
func TestPerformanceEventBus(t *testing.T) {
	output := perftest.Require(t)
	t.Setenv("PERF_NAMESPACE", fmt.Sprintf("performance_%d", time.Now().UnixNano()))
	bus, driver := performanceBus(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	var ackMu sync.Mutex
	var last [10]atomic.Uint64
	for i := range last {
		last[i].Store(uint64(i))
	}
	var failures atomic.Uint64
	if err := bus.HandleTask(performanceTask, eventbus.Handle(func(_ context.Context, _ eventbus.Job, p performancePayload) error {
		index := p.Sequence % 10
		old := last[index].Load()
		if p.Sequence != old+10 || !last[index].CompareAndSwap(old, p.Sequence) || len(p.Data) != 1024 {
			failures.Add(1)
			return eventbus.MarkPermanent(fmt.Errorf("duplicate/invalid message"))
		}
		var ack [8]byte
		binary.LittleEndian.PutUint64(ack[:], p.Sequence)
		ackMu.Lock()
		_, err := writer.Write(ack[:])
		ackMu.Unlock()
		return err
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	err = startBus(ctx, driver, bus)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPerformanceEventBusProducer$", "-test.timeout=4m")
	cmd.Stdin = reader
	result := perftest.Child(t, "eventbus-"+driver, cmd)
	ctx, cancel = context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := bus.DrainTopology(ctx, performanceTask); err != nil {
		t.Fatal(err)
	}
	stats, err := bus.TopologyStats(ctx, eventbus.TaskRef(performanceTask))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFinalStats(stats); err != nil {
		t.Fatal(err)
	}
	if failures.Load() != 0 {
		t.Fatal("duplicate/invalid messages")
	}
	result.Pending, result.InFlight, result.Delayed, result.DLQ = stats.Pending, stats.InFlight, stats.Delayed, stats.DLQ
	if err := perftest.WriteJSON(filepath.Join(output, "metrics.json"), []perftest.Result{result}); err != nil {
		t.Fatal(err)
	}
}
func TestPerformanceEventBusProducer(t *testing.T) {
	if os.Getenv("PERF_PHASE_DIR") == "" {
		t.Skip("producer subprocess only")
	}
	bus, driver := performanceBus(t)
	var slots [10]chan uint64
	for i := range slots {
		slots[i] = make(chan uint64, 1)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var ack [8]byte
		for {
			if _, err := io.ReadFull(os.Stdin, ack[:]); err != nil {
				return
			}
			seq := binary.LittleEndian.Uint64(ack[:])
			select {
			case slots[seq%10] <- seq:
			case <-t.Context().Done():
				return
			}
		}
	}()
	operations := make([]func(context.Context) error, 10)
	for i := range operations {
		seq := uint64(i)
		data := strings.Repeat("x", 1024)
		operations[i] = func(ctx context.Context) error {
			seq += 10
			if err := bus.Enqueue(ctx, performanceTask, performancePayload{Sequence: seq, Data: data}); err != nil {
				return err
			}
			select {
			case got := <-slots[i]:
				if got != seq {
					return fmt.Errorf("duplicate/out-of-order acknowledgement")
				}
				return nil
			case <-done:
				return fmt.Errorf("acknowledgement pipe closed")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	result, err := perftest.Load("eventbus-"+driver, operations)
	if err != nil {
		t.Fatal(err)
	}
	if err := perftest.WriteJSON(filepath.Join(os.Getenv("PERF_PHASE_DIR"), "client.json"), result); err != nil {
		t.Fatal(err)
	}
}
