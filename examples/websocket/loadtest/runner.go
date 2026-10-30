package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type options struct {
	target, token, tenant, account, event, scenario                     string
	connections, messages, reconnects, slowReaders, groupSize, accounts int
	hold, churnDelay                                                    time.Duration
	rate                                                                float64
}

func (o options) validate() error {
	u, err := url.Parse(o.target)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("target must be a ws/wss URL without credentials, query or fragment")
	}
	if o.token == "" || o.tenant == "" || o.account == "" || o.event == "" {
		return errors.New("token, tenant, account and event are required")
	}
	if o.connections < 1 || o.connections > 100000 || o.messages < 0 || o.messages > 100000 || o.reconnects < 1 || o.reconnects > 1000 || o.accounts < 1 || o.accounts > o.connections || o.slowReaders < 0 || o.slowReaders > 100 || o.hold < 0 || o.churnDelay < 0 || math.IsNaN(o.rate) || math.IsInf(o.rate, 0) || o.rate < 0 || o.rate > 1000000 || (o.rate > 0 && o.rate < 0.001) {
		return errors.New("invalid workload limits (connections <= 100000, messages <= 100000, cycles <= 1000, rate 0 or 0.001..1000000)")
	}
	switch o.scenario {
	case "message", "idle", "churn", "reconnect", "rolling-drain":
	case "fanout":
		if o.groupSize < 2 || o.groupSize > 100 || o.connections%o.groupSize != 0 {
			return errors.New("fanout requires group-size 2..100 and connections divisible by group-size")
		}
	default:
		return errors.New("unknown scenario")
	}
	if o.expected() > 10000000 {
		return errors.New("workload exceeds 10000000 expected deliveries; split it into runs")
	}
	return nil
}

func (o options) cycles() int {
	if o.scenario == "churn" || o.scenario == "reconnect" {
		return o.reconnects
	}
	return 1
}

func (o options) expected() int64 {
	if o.scenario == "idle" || o.scenario == "rolling-drain" || o.scenario == "churn" {
		return 0
	}
	n := int64(o.connections) * int64(o.messages) * int64(o.cycles())
	if o.scenario == "fanout" {
		n *= int64(o.groupSize)
	}
	return n
}

type result struct {
	Scenario               string  `json:"scenario"`
	Connections            int     `json:"connections"`
	MessagesPerConnection  int     `json:"messagesPerConnection"`
	GroupSize              int     `json:"groupSize"`
	Rate                   float64 `json:"messagesPerSecondPerConnection"`
	Duration               string  `json:"duration"`
	Connected              int64   `json:"successfulConnectionsCumulative"`
	Active                 int64   `json:"activeConnectionsAtEnd"`
	PeakActive             int64   `json:"peakActiveConnections"`
	ConnectErrors          int64   `json:"connectErrors"`
	Sent                   int64   `json:"sent"`
	Received               int64   `json:"received"`
	Expected               int64   `json:"expectedReceipts"`
	Missing                int64   `json:"missingReceipts"`
	Duplicates             int64   `json:"duplicateReceipts"`
	MessageErrors          int64   `json:"messageErrors"`
	LatencySamples         int     `json:"latencySamples"`
	P50Millis              float64 `json:"p50Millis"`
	P95Millis              float64 `json:"p95Millis"`
	P99Millis              float64 `json:"p99Millis"`
	ClientGoroutinesBefore int     `json:"clientGoroutinesBefore"`
	ClientGoroutinesAfter  int     `json:"clientGoroutinesAfter"`
	ClientHeapBefore       uint64  `json:"clientHeapBeforeBytes"`
	ClientHeapAfter        uint64  `json:"clientHeapAfterBytes"`
}

type delivery struct {
	start  time.Time
	sender int
}
type runner struct {
	o           options
	metrics     result
	mu          sync.Mutex
	latencies   []time.Duration
	pending     map[string]delivery
	runID       string
	receiptDone chan struct{}
}

type client struct {
	conn     *websocket.Conn
	done     chan struct{}
	replies  chan string
	stopping atomic.Bool
}

func run(o options) result {
	r := &runner{o: o, metrics: result{Scenario: o.scenario, Connections: o.connections, MessagesPerConnection: o.messages, GroupSize: o.groupSize, Rate: o.rate, Expected: o.expected()}, pending: make(map[string]delivery), runID: fmt.Sprint(time.Now().UnixNano()), receiptDone: make(chan struct{})}
	if r.metrics.Expected == 0 {
		close(r.receiptDone)
	}
	start := time.Now()
	var wg, ready sync.WaitGroup
	ready.Add(o.connections)
	// Every member must acknowledge room.join before any fanout starts.
	for id := 0; id < o.connections; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for cycle := 0; cycle < o.cycles(); cycle++ {
				c := r.connect(id, cycle)
				if o.scenario == "fanout" {
					if c != nil && !r.request(c, "room.join", map[string]any{"room_id": r.room(id)}, "room.joined") {
						r.close(c)
						c = nil
					}
					ready.Done()
					ready.Wait()
				}
				if c == nil {
					return
				}
				if o.scenario == "fanout" && (atomic.LoadInt64(&r.metrics.ConnectErrors) > 0 || atomic.LoadInt64(&r.metrics.MessageErrors) > 0) {
					r.close(c)
					return
				}
				r.send(c, id)
				if o.scenario == "fanout" {
					timer := time.NewTimer(10 * time.Second)
					select {
					case <-r.receiptDone:
					case <-c.done:
					case <-timer.C:
					}
					timer.Stop()
				}
				if o.hold > 0 {
					select {
					case <-time.After(o.hold):
					case <-c.done:
					}
				}
				r.close(c)
				if cycle+1 < o.cycles() {
					time.Sleep(o.churnDelay)
				}
			}
		}(id)
	}
	wg.Wait()
	r.metrics.Duration = time.Since(start).String()
	r.metrics.Missing = r.metrics.Expected - r.metrics.Received
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	r.metrics.LatencySamples = len(r.latencies)
	r.metrics.P50Millis = percentile(r.latencies, .5)
	r.metrics.P95Millis = percentile(r.latencies, .95)
	r.metrics.P99Millis = percentile(r.latencies, .99)
	return r.metrics
}

func (r *runner) room(id int) string {
	return fmt.Sprintf("%s:load-%s-%d", r.o.tenant, r.runID, id/r.o.groupSize)
}

func (r *runner) connect(id, cycle int) *client {
	u, _ := url.Parse(r.o.target)
	q := u.Query()
	q.Set("user_id", fmt.Sprintf("load-%s-%d", r.runID, id))
	q.Set("tenant_id", r.o.tenant)
	account := r.o.account
	if r.o.accounts > 1 {
		account = fmt.Sprintf("%s-%d", account, id%r.o.accounts)
	}
	q.Set("account_id", account)
	session := fmt.Sprintf("load-%s-%d", r.runID, id)
	if r.o.scenario == "churn" {
		session = fmt.Sprintf("%s-%d", session, cycle)
	}
	q.Set("session_id", session)
	u.RawQuery = q.Encode()
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.Dial(u.String(), http.Header{"Authorization": []string{"Bearer " + r.o.token}})
	if err != nil {
		atomic.AddInt64(&r.metrics.ConnectErrors, 1)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil
	}
	conn.SetReadLimit(64 * 1024)
	c := &client{conn: conn, done: make(chan struct{}), replies: make(chan string, 16)}
	atomic.AddInt64(&r.metrics.Connected, 1)
	active := atomic.AddInt64(&r.metrics.Active, 1)
	for peak := atomic.LoadInt64(&r.metrics.PeakActive); active > peak; peak = atomic.LoadInt64(&r.metrics.PeakActive) {
		if atomic.CompareAndSwapInt64(&r.metrics.PeakActive, peak, active) {
			break
		}
	}
	go r.read(c, id)
	return c
}

func (r *runner) read(c *client, id int) {
	defer close(c.done)
	defer atomic.AddInt64(&r.metrics.Active, -1)
	seen := make(map[string]bool)
	for {
		_, payload, err := c.conn.ReadMessage() // Processes control frames and answers server pings during idle too.
		if err != nil {
			if !c.stopping.Load() {
				atomic.AddInt64(&r.metrics.MessageErrors, 1)
			}
			return
		}
		if id%100 < r.o.slowReaders {
			time.Sleep(100 * time.Millisecond)
		}
		var message struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(payload, &message) != nil {
			atomic.AddInt64(&r.metrics.MessageErrors, 1)
			continue
		}
		if message.Type == "session.info" {
			continue
		}
		if message.Type == "error" {
			atomic.AddInt64(&r.metrics.MessageErrors, 1)
		}
		if r.o.scenario == "fanout" && message.Type == "chat.message" {
			var text string
			if json.Unmarshal(message.Data, &text) != nil {
				var data struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(message.Data, &data)
				text = data.Text
			}
			r.mu.Lock()
			d, ok := r.pending[text]
			if ok && d.sender/r.o.groupSize == id/r.o.groupSize {
				if seen[text] {
					atomic.AddInt64(&r.metrics.Duplicates, 1)
				} else {
					seen[text] = true
					if atomic.AddInt64(&r.metrics.Received, 1) == r.metrics.Expected {
						close(r.receiptDone)
					}
					r.sample(time.Since(d.start))
				}
			} else {
				atomic.AddInt64(&r.metrics.MessageErrors, 1)
			}
			r.mu.Unlock()
			continue
		}
		select {
		case c.replies <- message.Type:
		default:
			atomic.AddInt64(&r.metrics.MessageErrors, 1)
		}
	}
}

// sample is called with mu held; results expose the capped sample count.
func (r *runner) sample(d time.Duration) {
	if len(r.latencies) < 100000 {
		r.latencies = append(r.latencies, d)
	}
}

func (r *runner) write(c *client, event string, data any) bool {
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if c.conn.WriteJSON(map[string]any{"type": event, "data": data}) != nil {
		atomic.AddInt64(&r.metrics.MessageErrors, 1)
		return false
	}
	return true
}

func (r *runner) request(c *client, event string, data any, reply string) bool {
	if !r.write(c, event, data) {
		return false
	}
	if event != "room.join" {
		atomic.AddInt64(&r.metrics.Sent, 1)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case kind := <-c.replies:
			if kind == reply || reply == "" && kind != "error" {
				return true
			}
			if kind == "error" {
				return false
			}
		case <-c.done:
			return false
		case <-timer.C:
			atomic.AddInt64(&r.metrics.MessageErrors, 1)
			return false
		}
	}
}

func (r *runner) send(c *client, id int) {
	if r.o.scenario == "idle" || r.o.scenario == "rolling-drain" || r.o.scenario == "churn" {
		return
	}
	start := time.Now()
	for seq := 0; seq < r.o.messages; seq++ {
		if r.o.rate > 0 {
			wait := time.Until(start.Add(time.Duration(float64(seq) / r.o.rate * float64(time.Second))))
			if wait > 0 {
				select {
				case <-time.After(wait):
				case <-c.done:
					return
				}
			}
		}
		before := time.Now()
		if r.o.scenario == "fanout" {
			text := fmt.Sprintf("load-%s-%d-%d", r.runID, id, seq)
			r.mu.Lock()
			r.pending[text] = delivery{start: before, sender: id}
			r.mu.Unlock()
			if !r.write(c, "chat.send", map[string]any{"room_id": r.room(id), "text": text}) {
				return
			}
			atomic.AddInt64(&r.metrics.Sent, 1)
		} else {
			reply := ""
			if r.o.event == "system.ping" {
				reply = "system.pong"
			}
			if !r.request(c, r.o.event, map[string]any{"client": id, "sequence": seq}, reply) {
				return
			}
			atomic.AddInt64(&r.metrics.Received, 1)
			r.mu.Lock()
			r.sample(time.Since(before))
			r.mu.Unlock()
		}
	}
}

func (r *runner) close(c *client) {
	c.stopping.Store(true)
	_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "load complete"), time.Now().Add(time.Second))
	_ = c.conn.Close()
	<-c.done
}
