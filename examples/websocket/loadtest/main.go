package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/thanhbvha/go-common/examples/websocket/internal/exampleconfig"
)

type result struct {
	Target                string  `json:"target"`
	Scenario              string  `json:"scenario"`
	Connections           int     `json:"connections"`
	MessagesPerConnection int     `json:"messagesPerConnection"`
	Duration              string  `json:"duration"`
	Connected             int64   `json:"connected"`
	ConnectErrors         int64   `json:"connectErrors"`
	FirstConnectError     string  `json:"firstConnectError,omitempty"`
	Sent                  int64   `json:"sent"`
	Received              int64   `json:"received"`
	MessageErrors         int64   `json:"messageErrors"`
	P50Millis             float64 `json:"p50Millis"`
	P95Millis             float64 `json:"p95Millis"`
	P99Millis             float64 `json:"p99Millis"`
	GoroutinesBefore      int     `json:"goroutinesBefore"`
	GoroutinesAfter       int     `json:"goroutinesAfter"`
	HeapBefore            uint64  `json:"heapBefore"`
	HeapAfter             uint64  `json:"heapAfter"`
}

func main() {
	configPath := exampleconfig.Path(os.Args[1:])
	loaded, err := exampleconfig.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config %q: %v\n", configPath, err)
		os.Exit(1)
	}
	configFile := flag.String("config", configPath, "YAML or .env configuration file")
	target := flag.String("target", loaded.LoadTarget, "WebSocket URL")
	token := flag.String("token", loaded.ClientToken, "Bearer token")
	tenant := flag.String("tenant", loaded.ClientTenant, "Tenant ID")
	account := flag.String("account", loaded.ClientAccount, "Account ID")
	connections := flag.Int("connections", loaded.LoadConnections, "Concurrent connections")
	messages := flag.Int("messages", loaded.LoadMessages, "Messages per connection")
	hold := flag.Duration("hold", loaded.LoadHold, "Idle hold after sending")
	slowReaders := flag.Int("slow-readers", loaded.LoadSlowReaders, "Percentage of clients delaying reads")
	eventType := flag.String("event", loaded.LoadEvent, "Application event type")
	scenario := flag.String("scenario", loaded.LoadScenario, "Scenario: message, idle, churn, fanout, reconnect, rolling-drain")
	reconnects := flag.Int("reconnects", loaded.LoadReconnects, "Reconnect cycles for reconnect and churn scenarios")
	churnDelay := flag.Duration("churn-delay", loaded.LoadChurnDelay, "Delay between churn or reconnect cycles")
	output := flag.String("output", loaded.LoadOutput, "Optional JSON result path")
	flag.Parse()
	_ = configFile
	if *token == "" {
		fmt.Fprintln(os.Stderr, "client_token is required in config or WS_EXAMPLE_CLIENT_TOKEN")
		os.Exit(1)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	metrics := result{Target: *target, Scenario: *scenario, Connections: *connections, MessagesPerConnection: *messages, GoroutinesBefore: runtime.NumGoroutine(), HeapBefore: before.HeapAlloc}
	latencies := make([]time.Duration, 0, *connections**messages)
	var latencyMu sync.Mutex
	var connectErrorOnce sync.Once
	var wg sync.WaitGroup
	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			cycles := 1
			if *scenario == "churn" || *scenario == "reconnect" {
				cycles = *reconnects
			}
			for cycle := 0; cycle < cycles; cycle++ {
				targetURL, err := url.Parse(*target)
				if err != nil {
					atomic.AddInt64(&metrics.ConnectErrors, 1)
					return
				}
				query := targetURL.Query()
				query.Set("user_id", fmt.Sprintf("load-%d", clientID))
				query.Set("tenant_id", *tenant)
				query.Set("account_id", *account)
				query.Set("session_id", fmt.Sprintf("load-%d-%d", clientID, cycle))
				targetURL.RawQuery = query.Encode()
				header := http.Header{"Authorization": []string{"Bearer " + *token}}
				conn, response, err := websocket.DefaultDialer.Dial(targetURL.String(), header)
				if err != nil {
					atomic.AddInt64(&metrics.ConnectErrors, 1)
					connectErrorOnce.Do(func() {
						metrics.FirstConnectError = err.Error()
						if response != nil {
							metrics.FirstConnectError = fmt.Sprintf("%s; HTTP status=%d", err, response.StatusCode)
						}
					})
					if response != nil {
						_ = response.Body.Close()
					}
					return
				}
				atomic.AddInt64(&metrics.Connected, 1)
				if *scenario == "idle" || *scenario == "rolling-drain" {
					time.Sleep(*hold)
					closeGracefully(conn, "load scenario complete")
					continue
				}
				messageCount := *messages
				if *scenario == "churn" {
					messageCount = 0
				}
				for sequence := 0; sequence < messageCount; sequence++ {
					payload := []byte(fmt.Sprintf(`{"type":%q,"data":{"client":%d,"sequence":%d}}`, *eventType, clientID, sequence))
					start := time.Now()
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						atomic.AddInt64(&metrics.MessageErrors, 1)
						_ = conn.Close()
						return
					}
					atomic.AddInt64(&metrics.Sent, 1)
					if clientID%100 < *slowReaders {
						time.Sleep(100 * time.Millisecond)
					}
					_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					if _, _, err := conn.ReadMessage(); err != nil {
						atomic.AddInt64(&metrics.MessageErrors, 1)
						_ = conn.Close()
						return
					}
					atomic.AddInt64(&metrics.Received, 1)
					latencyMu.Lock()
					latencies = append(latencies, time.Since(start))
					latencyMu.Unlock()
				}
				if *hold > 0 {
					time.Sleep(*hold)
				}
				closeGracefully(conn, "load scenario complete")
				if cycle+1 < cycles {
					time.Sleep(*churnDelay)
				}
			}
		}(i)
	}
	wg.Wait()
	metrics.Duration = time.Since(started).String()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	metrics.P50Millis = percentile(latencies, 0.50)
	metrics.P95Millis = percentile(latencies, 0.95)
	metrics.P99Millis = percentile(latencies, 0.99)
	runtime.GC()
	runtime.ReadMemStats(&after)
	metrics.GoroutinesAfter = runtime.NumGoroutine()
	metrics.HeapAfter = after.HeapAlloc
	encoded, _ := json.MarshalIndent(metrics, "", "  ")
	if *output != "" {
		if err := os.WriteFile(*output, encoded, 0o644); err != nil {
			panic(err)
		}
	}
	fmt.Println(string(encoded))
	if metrics.ConnectErrors > 0 || metrics.MessageErrors > 0 {
		os.Exit(1)
	}
}

func closeGracefully(conn *websocket.Conn, reason string) {
	deadline := time.Now().Add(time.Second)
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason), deadline)
	_ = conn.Close()
}

func percentile(values []time.Duration, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return float64(values[int(float64(len(values)-1)*fraction)]) / float64(time.Millisecond)
}
