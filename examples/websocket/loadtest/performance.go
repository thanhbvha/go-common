package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/internal/perftest"
)

type performanceMessage struct {
	kind string
	data json.RawMessage
}

// runPerformance reuses the existing connection, authentication, reader and
// protocol paths. It bounds outstanding work to one message per client/group.
func runPerformance(o options) error {
	if o.scenario != "message" && o.scenario != "fanout" {
		return fmt.Errorf("performance mode supports message/fanout")
	}
	r := &runner{o: o, performance: true, runID: fmt.Sprint(time.Now().UnixNano())}
	var sent, expected, received atomic.Uint64
	clients := make([]*client, 0, o.connections)
	defer func() {
		for _, c := range clients {
			r.close(c)
		}
	}()
	expect := func(ctx context.Context, c *client, kind, text string) error {
		select {
		case m := <-c.performanceMessages:
			if m.kind != kind {
				return fmt.Errorf("unexpected response: %s, want %s", m.kind, kind)
			}
			if text != "" {
				var got string
				if kind == "business.completed" {
					var body struct {
						Text string `json:"text"`
					}
					if err := json.Unmarshal(m.data, &body); err != nil {
						return err
					}
					got = body.Text
				} else if err := json.Unmarshal(m.data, &got); err != nil {
					return err
				}
				if got != text {
					return fmt.Errorf("wrong/duplicate fanout receipt")
				}
			}
			received.Add(1)
			return nil
		case <-c.done:
			return fmt.Errorf("connection closed")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for id := range o.connections {
		c := r.connect(id, 0)
		if c == nil {
			return fmt.Errorf("connect failed")
		}
		clients = append(clients, c)
		if o.scenario == "fanout" {
			if !r.write(c, "room.join", map[string]any{"room_id": r.room(id)}) {
				return fmt.Errorf("room join write")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := expect(ctx, c, "room.joined", "")
			cancel()
			if err != nil {
				return err
			}
		}
	}
	// Room join acknowledgements are setup, not workload receipts.
	received.Store(0)
	var operations []func(context.Context) error
	if o.scenario == "message" {
		for id, c := range clients {
			var seq uint64
			operations = append(operations, func(ctx context.Context) error {
				seq++
				text := fmt.Sprintf("%d-%d", id, seq)
				if !r.write(c, "business", map[string]any{"text": text}) {
					return fmt.Errorf("ping write")
				}
				sent.Add(1)
				expected.Add(1)
				return expect(ctx, c, "business.completed", text)
			})
		}
	} else {
		for first := 0; first < len(clients); first += o.groupSize {
			members := clients[first : first+o.groupSize]
			room := r.room(first)
			var seq uint64
			operations = append(operations, func(ctx context.Context) error {
				seq++
				text := fmt.Sprintf("%s-%s-%d", r.runID, room, seq)
				if !r.write(members[0], "chat.send", map[string]any{"room_id": room, "text": text}) {
					return fmt.Errorf("chat write")
				}
				sent.Add(1)
				expected.Add(uint64(len(members)))
				for _, c := range members {
					if err := expect(ctx, c, "chat.message", text); err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
	result, err := perftest.LoadPaced("websocket-"+o.scenario, operations, 10)
	if err != nil {
		return err
	}
	phase := os.Getenv("PERF_PHASE_DIR")
	if err := os.WriteFile(filepath.Join(phase, "load-complete"), nil, 0600); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := perftest.Await(ctx, filepath.Join(phase, "broker-drained")); err != nil {
		return err
	}
	// Observe late/duplicate replies through broker drain and a bounded quiet
	// window. Counts include warmup + measurement; latency excludes this window.
	quiet := time.NewTimer(100 * time.Millisecond)
	defer quiet.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
quietWindow:
	for {
		for _, c := range clients {
			select {
			case <-c.performanceMessages:
				return fmt.Errorf("duplicate/unsolicited delivery during drain")
			case <-c.done:
				return fmt.Errorf("connection closed during drain")
			default:
			}
		}
		select {
		case <-quiet.C:
			break quietWindow
		case <-tick.C:
		}
	}
	result.SentOperations, result.ExpectedReceipts, result.Received = sent.Load(), expected.Load(), received.Load()
	if result.SentOperations == 0 || result.Received != result.ExpectedReceipts {
		return fmt.Errorf("receipt count mismatch")
	}
	if atomic.LoadInt64(&r.metrics.MessageErrors) != 0 || atomic.LoadInt64(&r.metrics.ConnectErrors) != 0 {
		return fmt.Errorf("reader/connection errors")
	}
	// Any extra queued reply is a duplicate/unsolicited delivery.
	for _, c := range clients {
		if len(c.performanceMessages) != 0 {
			return fmt.Errorf("undrained replies")
		}
	}
	return perftest.WriteJSON(filepath.Join(os.Getenv("PERF_PHASE_DIR"), "client.json"), result)
}
