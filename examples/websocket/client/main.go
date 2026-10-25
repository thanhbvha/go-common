package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/thanhbvha/go-common/examples/websocket/internal/exampleconfig"
)

type clientConfig struct {
	serverURL  string
	userID     string
	tenantID   string
	accountID  string
	sessionID  string
	token      string
	adminToken string
	mode       string
	roomID     string
	message    string
	targetUser string
	probe      bool
}

type incomingEnvelope struct {
	Type      string `json:"type"`
	Data      any    `json:"data"`
	RequestID string `json:"request_id,omitempty"`
}

type outgoingEnvelope struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type demoClient struct {
	config   clientConfig
	conn     *websocket.Conn
	writeMu  sync.Mutex
	done     chan struct{}
	doneOnce sync.Once
}

func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := run(config); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseFlags() (clientConfig, error) {
	configPath := exampleconfig.Path(os.Args[1:])
	loaded, err := exampleconfig.Load(configPath)
	if err != nil {
		return clientConfig{}, fmt.Errorf("load config %q: %w", configPath, err)
	}
	configFile := flag.String("config", configPath, "YAML or .env configuration file")
	serverURL := flag.String("url", loaded.ClientURL, "WebSocket URL")
	userID := flag.String("user", loaded.ClientUser, "Identity user ID")
	tenantID := flag.String("tenant", loaded.ClientTenant, "Identity tenant ID")
	accountID := flag.String("account", loaded.ClientAccount, "Identity account ID; use admin for cluster.broadcast")
	sessionID := flag.String("session", loaded.ClientSession, "Identity session ID; generated when omitted")
	token := flag.String("token", loaded.ClientToken, "Client bearer token")
	adminToken := flag.String("admin-token", loaded.AdminToken, "Admin bearer token for /admin endpoints")
	mode := flag.String("mode", loaded.ClientMode, "Mode: interactive, script, or single")
	roomID := flag.String("room", loaded.ClientRoom, "Room used by script and chat commands")
	message := flag.String("message", loaded.ClientMessage, "Message used by script and single mode")
	targetUser := flag.String("target-user", loaded.ClientTargetUser, "Target user for notification command")
	probe := flag.Bool("probe", loaded.ClientProbe, "Probe /health and /admin/stats before connecting")
	flag.Parse()
	_ = configFile

	if *sessionID == "" {
		*sessionID = fmt.Sprintf("%s-%d", *userID, time.Now().UnixNano())
	}
	if *adminToken == "" {
		*adminToken = *token
	}
	return clientConfig{serverURL: *serverURL, userID: *userID, tenantID: *tenantID, accountID: *accountID, sessionID: *sessionID, token: *token, adminToken: *adminToken, mode: *mode, roomID: *roomID, message: *message, targetUser: *targetUser, probe: *probe}, nil
}

func run(config clientConfig) error {
	if config.token == "" {
		return errors.New("client_token is required in config, environment, or -token")
	}
	if config.userID == "" || config.tenantID == "" {
		return errors.New("user and tenant are required")
	}
	if config.mode != "interactive" && config.mode != "script" && config.mode != "single" {
		return fmt.Errorf("invalid mode %q", config.mode)
	}
	if config.probe {
		probeHTTP(config)
	}

	client, err := connect(config)
	if err != nil {
		return err
	}
	defer client.close("client exit")
	go client.readLoop()

	fmt.Printf("connected user=%s tenant=%s account=%s session=%s\n", config.userID, config.tenantID, config.accountID, config.sessionID)
	switch config.mode {
	case "interactive":
		return client.runInteractive()
	case "script":
		return client.runScript()
	case "single":
		if err := client.send("chat.send", map[string]any{"room_id": config.roomID, "text": config.message}); err != nil {
			return err
		}
		return client.wait(2 * time.Second)
	}
	return nil
}

func connect(config clientConfig) (*demoClient, error) {
	target, err := url.Parse(config.serverURL)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	query := target.Query()
	query.Set("user_id", config.userID)
	query.Set("tenant_id", config.tenantID)
	query.Set("account_id", config.accountID)
	query.Set("session_id", config.sessionID)
	target.RawQuery = query.Encode()

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+config.token)
	headers.Set("X-Request-ID", "client-"+config.sessionID)
	conn, response, err := websocket.DefaultDialer.Dial(target.String(), headers)
	if err != nil {
		if response != nil {
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			return nil, fmt.Errorf("upgrade failed: %w; status=%d body=%s", err, response.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil, fmt.Errorf("dial: %w", err)
	}
	return &demoClient{config: config, conn: conn, done: make(chan struct{})}, nil
}

func (c *demoClient) readLoop() {
	defer c.doneOnce.Do(func() { close(c.done) })
	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				fmt.Println("connection closed:", err)
			}
			return
		}
		var envelope incomingEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			fmt.Printf("<- raw %s\n", payload)
			continue
		}
		encoded, _ := json.MarshalIndent(envelope.Data, "", "  ")
		fmt.Printf("<- %s request_id=%s\n%s\n", envelope.Type, envelope.RequestID, encoded)
	}
}

func (c *demoClient) send(eventType string, data any) error {
	payload, err := json.Marshal(outgoingEnvelope{Type: eventType, Data: data})
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return err
	}
	fmt.Printf("-> %s %s\n", eventType, payload)
	return nil
}

func (c *demoClient) runScript() error {
	steps := []struct {
		eventType string
		data      any
		wait      time.Duration
	}{
		{"session.info", map[string]any{}, 250 * time.Millisecond},
		{"system.ping", map[string]any{}, 250 * time.Millisecond},
		{"room.join", map[string]any{"room_id": c.config.roomID}, 250 * time.Millisecond},
		{"chat.send", map[string]any{"room_id": c.config.roomID, "text": c.config.message}, 500 * time.Millisecond},
		{"user.notify", map[string]any{"user_id": c.config.targetUser, "text": "direct notification from " + c.config.userID}, 500 * time.Millisecond},
	}
	if c.config.accountID == "admin" {
		steps = append(steps, struct {
			eventType string
			data      any
			wait      time.Duration
		}{"cluster.broadcast", map[string]any{"text": "cluster broadcast from " + c.config.userID}, 500 * time.Millisecond})
	}
	steps = append(steps, struct {
		eventType string
		data      any
		wait      time.Duration
	}{"room.leave", map[string]any{"room_id": c.config.roomID}, 250 * time.Millisecond})
	for _, step := range steps {
		if err := c.send(step.eventType, step.data); err != nil {
			return err
		}
		if err := c.wait(step.wait); err != nil {
			return err
		}
	}
	return nil
}

func (c *demoClient) runInteractive() error {
	printHelp()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	for {
		fmt.Print("ws> ")
		select {
		case <-interrupt:
			return nil
		case <-c.done:
			return nil
		case line, ok := <-lines:
			if !ok {
				return nil
			}
			quit, err := c.handleCommand(strings.TrimSpace(line))
			if err != nil {
				fmt.Println("command error:", err)
			}
			if quit {
				return nil
			}
		}
	}
}

func (c *demoClient) handleCommand(line string) (bool, error) {
	if line == "" {
		return false, nil
	}
	command, arguments, _ := strings.Cut(line, " ")
	arguments = strings.TrimSpace(arguments)
	switch strings.ToLower(command) {
	case "/help":
		printHelp()
	case "/quit", "/exit":
		return true, nil
	case "/info":
		return false, c.send("session.info", map[string]any{})
	case "/ping":
		return false, c.send("system.ping", map[string]any{})
	case "/join":
		if arguments == "" {
			return false, errors.New("usage: /join <tenant:room>")
		}
		return false, c.send("room.join", map[string]any{"room_id": arguments})
	case "/leave":
		if arguments == "" {
			return false, errors.New("usage: /leave <tenant:room>")
		}
		return false, c.send("room.leave", map[string]any{"room_id": arguments})
	case "/chat":
		roomID, text, ok := strings.Cut(arguments, " ")
		if !ok || roomID == "" || strings.TrimSpace(text) == "" {
			return false, errors.New("usage: /chat <tenant:room> <text>")
		}
		return false, c.send("chat.send", map[string]any{"room_id": roomID, "text": strings.TrimSpace(text)})
	case "/notify":
		userID, text, ok := strings.Cut(arguments, " ")
		if !ok || userID == "" || strings.TrimSpace(text) == "" {
			return false, errors.New("usage: /notify <user> <text>")
		}
		return false, c.send("user.notify", map[string]any{"user_id": userID, "text": strings.TrimSpace(text)})
	case "/broadcast":
		if arguments == "" {
			return false, errors.New("usage: /broadcast <text>; account must be admin")
		}
		return false, c.send("cluster.broadcast", map[string]any{"text": arguments})
	case "/raw":
		var envelope outgoingEnvelope
		if err := json.Unmarshal([]byte(arguments), &envelope); err != nil {
			return false, fmt.Errorf("invalid JSON: %w", err)
		}
		return false, c.send(envelope.Type, envelope.Data)
	default:
		return false, c.send("chat.send", map[string]any{"room_id": c.config.roomID, "text": line})
	}
	return false, nil
}

func printHelp() {
	fmt.Println(`commands:
  /info                              show structured identity and shard
  /ping                              request system.pong
  /join <tenant:room>                join an authorized room
  /leave <tenant:room>               leave a room
  /chat <tenant:room> <text>         fan out a room message
  /notify <user> <text>              notify all local/cluster sessions for a user
  /broadcast <text>                  shard-wide cluster broadcast; account=admin required
  /raw {"type":"...","data":{}}     send an arbitrary envelope
  /help                              print this help
  /quit                              close the session

Any line without a slash is sent as chat.send to the default -room.`)
}

func (c *demoClient) wait(duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-c.done:
		return errors.New("connection closed")
	case <-timer.C:
		return nil
	}
}

func (c *demoClient) close(reason string) {
	c.writeMu.Lock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason))
	c.writeMu.Unlock()
	_ = c.conn.Close()
}

func probeHTTP(config clientConfig) {
	websocketURL, err := url.Parse(config.serverURL)
	if err != nil {
		return
	}
	scheme := "http"
	if websocketURL.Scheme == "wss" {
		scheme = "https"
	}
	baseURL := scheme + "://" + websocketURL.Host
	client := &http.Client{Timeout: 3 * time.Second}
	probeEndpoint(client, baseURL+"/health", config.token)
	probeEndpoint(client, baseURL+"/admin/stats", config.adminToken)
}

func probeEndpoint(client *http.Client, endpoint, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		fmt.Printf("probe %s failed: %v\n", endpoint, err)
		return
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	fmt.Printf("probe %s -> %d %s\n", endpoint, response.StatusCode, strings.TrimSpace(string(body)))
}
