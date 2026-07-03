// Package main implements a premium interactive command-line client to test and verify
// the high-performance clustered WebSocket service. It works seamlessly with any of the
// three adapters (Fiber, Echo, Gin) running on the local workspace.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// IncomingMessage represents the envelope for incoming payloads sent to the WS server.
type IncomingMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// OutgoingMessage represents the envelope for outgoing payloads received from the WS server.
type OutgoingMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	RequestID string      `json:"request_id,omitempty"`
}

// EchoPayload represents the schema returned by the custom chat_message handler in the servers.
type EchoPayload struct {
	Sender  string    `json:"sender"`
	Payload string    `json:"payload"`
	SentAt  time.Time `json:"sent_at"`
}

func main() {
	// Parse CLI flags
	hostFlag := flag.String("host", "localhost", "WebSocket server hostname")
	portFlag := flag.Int("port", 8080, "WebSocket server port")
	userFlag := flag.String("user", "", "User ID for authentication (defaults to randomized ID)")
	interactiveFlag := flag.Bool("interactive", true, "Enable interactive mode to type messages in real time")
	probeFlag := flag.Bool("probe", true, "Perform HTTP health & stats pre-flight checks")
	flag.Parse()

	// Seed random generator for user ID
	rand.Seed(time.Now().UnixNano())
	userID := *userFlag
	if userID == "" {
		userID = fmt.Sprintf("client_%d", rand.Intn(9000)+1000)
	}

	fmt.Printf("=== CLUSTERED WEBSOCKET INTEGRATION TEST CLIENT ===\n")
	fmt.Printf("Target Host: %s\n", *hostFlag)
	fmt.Printf("Target Port: %d\n", *portFlag)
	fmt.Printf("User ID:    %s (Authenticated via Query Parameter)\n", userID)
	fmt.Println(strings.Repeat("-", 60))

	// 1. Pre-flight checks (HTTP endpoints)
	if *probeFlag {
		runPreflightChecks(*hostFlag, *portFlag)
	}

	// 2. Establish WebSocket connection
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	u := url.URL{
		Scheme:   "ws",
		Host:     fmt.Sprintf("%s:%d", *hostFlag, *portFlag),
		Path:     "/ws",
		RawQuery: "user_id=" + url.QueryEscape(userID),
	}

	fmt.Printf("\n[CONNECTING] Handshaking with %s...\n", u.String())
	c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Printf("[CONNECTION ERROR] Upgrade failed: %v\n", err)
		if resp != nil {
			fmt.Printf("HTTP response status: %d\n", resp.StatusCode)
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("Response details: %s\n", string(body))
		}
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf("[CONNECTED] Established connection to WebSocket cluster node successfully!\n\n")

	done := make(chan struct{})

	// Goroutine for handling incoming WebSocket messages
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Printf("\n[DISCONNECTED] Server initiated graceful connection close.\n")
				} else {
					fmt.Printf("\n[DISCONNECTED] Connection terminated: %v\n", err)
				}
				return
			}

			// Parse envelope
			var out OutgoingMessage
			if err := json.Unmarshal(message, &out); err != nil {
				fmt.Printf("[RAW INCOMING] %s\n", string(message))
				continue
			}

			// Format and print received events beautifully
			switch out.Type {
			case "chat_echo":
				// Re-marshal and unmarshal into EchoPayload to guarantee field parsing
				dataBytes, err := json.Marshal(out.Data)
				if err == nil {
					var echo EchoPayload
					if err := json.Unmarshal(dataBytes, &echo); err == nil {
						fmt.Printf("\n[ECHO RESPONSE]\n")
						fmt.Printf("  Sender : %s\n", echo.Sender)
						fmt.Printf("  Message: %s\n", echo.Payload)
						fmt.Printf("  Time   : %s\n", echo.SentAt.Format(time.RFC3339Nano))
						if out.RequestID != "" {
							fmt.Printf("  Req ID : %s\n", out.RequestID)
						}
						// Print inline prompt again if in interactive mode
						if *interactiveFlag {
							fmt.Print("\nSend chat message -> ")
						}
						continue
					}
				}
				// Fallback if data doesn't match EchoPayload structure exactly
				fmt.Printf("\n[INCOMING EVENT] Type: %s, Data: %+v\n", out.Type, out.Data)
			default:
				fmt.Printf("\n[INCOMING EVENT] Type: %s, Data: %+v\n", out.Type, out.Data)
			}
			if *interactiveFlag {
				fmt.Print("\nSend chat message -> ")
			}
		}
	}()

	// 3. Send initial ping event
	sendChatMessage(c, "Initial handshake message from test client!")

	// 4. Handle input or keep alive
	if *interactiveFlag {
		fmt.Printf("[INTERACTIVE MODE] Type any message below and press Enter to transmit to the server cluster.\n")
		fmt.Printf("Press Ctrl+C to exit gracefully.\n\n")

		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("Send chat message -> ")

		// Set up goroutine for stdin processing so we can interrupt it cleanly via channel selection
		inputChan := make(chan string)
		go func() {
			for scanner.Scan() {
				inputChan <- scanner.Text()
			}
			if err := scanner.Err(); err != nil {
				fmt.Printf("[INPUT ERROR] Error reading from stdin: %v\n", err)
			}
		}()

		for {
			select {
			case text := <-inputChan:
				trimmed := strings.TrimSpace(text)
				if trimmed == "" {
					fmt.Print("Send chat message -> ")
					continue
				}
				if strings.ToLower(trimmed) == "/exit" || strings.ToLower(trimmed) == "/quit" {
					gracefulClose(c)
					return
				}
				if err := sendChatMessage(c, trimmed); err != nil {
					fmt.Printf("[SEND ERROR] Failed to send message: %v\n", err)
				}
			case <-interrupt:
				fmt.Printf("\n[SHUTDOWN] Ctrl+C received, terminating session...\n")
				gracefulClose(c)
				return
			case <-done:
				return
			}
		}
	} else {
		// Non-interactive mode: Send a few periodic messages and exit
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		count := 1
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				count++
				msg := fmt.Sprintf("Periodic test message #%d sent at %s", count, t.Format("15:04:05"))
				if err := sendChatMessage(c, msg); err != nil {
					fmt.Printf("[SEND ERROR] Failed to send message: %v\n", err)
					return
				}
				if count >= 5 {
					fmt.Printf("\n[FINISHED] Completed 5 message transmissions. Closing connection.\n")
					gracefulClose(c)
					return
				}
			case <-interrupt:
				fmt.Printf("\n[SHUTDOWN] Ctrl+C received, terminating session...\n")
				gracefulClose(c)
				return
			}
		}
	}
}

// sendChatMessage packages and sends a chat_message event.
func sendChatMessage(c *websocket.Conn, content string) error {
	dataBytes, err := json.Marshal(content)
	if err != nil {
		return err
	}

	msg := IncomingMessage{
		Type: "chat_message",
		Data: dataBytes,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	fmt.Printf("[SENDING] chat_message: %s\n", content)
	return c.WriteMessage(websocket.TextMessage, payload)
}

// gracefulClose sends a WebSocket close control frame and waits briefly for server confirmation.
func gracefulClose(c *websocket.Conn) {
	fmt.Printf("[CLOSING] Sending close frame to cluster...\n")
	err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Client disconnecting"))
	if err != nil {
		fmt.Printf("Error sending close frame: %v\n", err)
		return
	}
	<-time.After(1 * time.Second)
	fmt.Println("Closing local TCP socket.")
}

// runPreflightChecks performs GET queries to `/health` and `/stats` endpoints.
func runPreflightChecks(host string, port int) {
	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := fmt.Sprintf("http://%s:%d", host, port)

	fmt.Printf("[PROBING] Performing pre-flight health check on %s...\n", baseURL)

	// 1. Probe health
	healthResp, err := client.Get(baseURL + "/health")
	if err != nil {
		fmt.Printf("  [WARNING] Health endpoint unreachable: %v\n", err)
	} else {
		defer healthResp.Body.Close()
		body, _ := io.ReadAll(healthResp.Body)
		fmt.Printf("  Health Status: %d %s (Body: %s)\n", healthResp.StatusCode, http.StatusText(healthResp.StatusCode), strings.TrimSpace(string(body)))
	}

	// 2. Probe stats (Try standard endpoint and Fiber-specific API group)
	statsURL := baseURL + "/stats"
	statsResp, err := client.Get(statsURL)
	if err != nil || statsResp.StatusCode == http.StatusNotFound {
		// Try Fiber Group Path
		statsURL = baseURL + "/api/ws/stats"
		statsResp, err = client.Get(statsURL)
	}

	if err != nil {
		fmt.Printf("  [WARNING] Stats endpoint unreachable: %v\n", err)
	} else {
		defer statsResp.Body.Close()
		body, _ := io.ReadAll(statsResp.Body)
		fmt.Printf("  Server Stats : %s\n", strings.TrimSpace(string(body)))
	}
}
