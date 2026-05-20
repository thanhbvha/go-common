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
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
)

// ANSI terminal color codes for premium visual experience (defined as vars to support fallback on older Windows terminals)
var (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[37m"
	colorBold   = "\033[1m"
)

// disableColors strips ANSI color codes to support plain text outputs
func disableColors() {
	colorReset = ""
	colorRed = ""
	colorGreen = ""
	colorYellow = ""
	colorBlue = ""
	colorPurple = ""
	colorCyan = ""
	colorGray = ""
	colorBold = ""
}

// initTerminal configures terminal colors and enables Virtual Terminal Processing on Windows
func initTerminal(enableColor bool) {
	if !enableColor {
		disableColors()
		return
	}

	if runtime.GOOS == "windows" {
		kernel32 := syscall.NewLazyDLL("kernel32.dll")
		setConsoleMode := kernel32.NewProc("SetConsoleMode")
		getConsoleMode := kernel32.NewProc("GetConsoleMode")

		stdoutHandle, err := syscall.GetStdHandle(syscall.STD_OUTPUT_HANDLE)
		if err == nil {
			var mode uint32
			ret, _, _ := getConsoleMode.Call(uintptr(stdoutHandle), uintptr(unsafe.Pointer(&mode)))
			if ret != 0 {
				// ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
				const enableVTProcessing = 0x0004
				mode |= enableVTProcessing
				setConsoleMode.Call(uintptr(stdoutHandle), uintptr(mode))
			} else {
				// Fallback if GetConsoleMode fails (e.g. non-TTY redirect), disable colors
				disableColors()
			}
		} else {
			disableColors()
		}
	}
}

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
	colorFlag := flag.Bool("color", true, "Enable ANSI colors in console output")
	flag.Parse()

	// Initialize terminal colors (enables Windows VT Processing)
	initTerminal(*colorFlag)

	// Seed random generator for user ID
	rand.Seed(time.Now().UnixNano())
	userID := *userFlag
	if userID == "" {
		userID = fmt.Sprintf("client_%d", rand.Intn(9000)+1000)
	}

	fmt.Printf("%s%s=== CLUSTERED WEBSOCKET INTEGRATION TEST CLIENT ===%s\n", colorBold, colorBlue, colorReset)
	fmt.Printf("%sTarget Host:%s %s\n", colorBold, colorReset, *hostFlag)
	fmt.Printf("%sTarget Port:%s %d\n", colorBold, colorReset, *portFlag)
	fmt.Printf("%sUser ID:    %s %s (Authenticated via Query Parameter)\n", colorBold, colorReset, userID)
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

	fmt.Printf("\n%s[CONNECTING]%s Handshaking with %s...\n", colorCyan, colorReset, u.String())
	c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Printf("%s[CONNECTION ERROR] Upgrade failed: %v%s\n", colorRed, err, colorReset)
		if resp != nil {
			fmt.Printf("HTTP response status: %d\n", resp.StatusCode)
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("Response details: %s\n", string(body))
		}
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf("%s[CONNECTED]%s Established connection to WebSocket cluster node successfully!\n\n", colorGreen, colorReset)

	done := make(chan struct{})

	// Goroutine for handling incoming WebSocket messages
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Printf("\n%s[DISCONNECTED]%s Server initiated graceful connection close.%s\n", colorYellow, colorYellow, colorReset)
				} else {
					fmt.Printf("\n%s[DISCONNECTED]%s Connection terminated: %v%s\n", colorRed, colorRed, err, colorReset)
				}
				return
			}

			// Parse envelope
			var out OutgoingMessage
			if err := json.Unmarshal(message, &out); err != nil {
				fmt.Printf("%s[RAW INCOMING]%s %s\n", colorGray, colorReset, string(message))
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
						fmt.Printf("\n%s[ECHO RESPONSE] %s%s\n", colorGreen, colorBold, colorReset)
						fmt.Printf("  Sender : %s%s%s\n", colorCyan, echo.Sender, colorReset)
						fmt.Printf("  Message: %s%s%s\n", colorBold, echo.Payload, colorReset)
						fmt.Printf("  Time   : %s%s%s\n", colorGray, echo.SentAt.Format(time.RFC3339Nano), colorReset)
						if out.RequestID != "" {
							fmt.Printf("  Req ID : %s%s%s\n", colorGray, out.RequestID, colorReset)
						}
						// Print inline prompt again if in interactive mode
						if *interactiveFlag {
							fmt.Print("\n" + colorCyan + "Send chat message -> " + colorReset)
						}
						continue
					}
				}
				// Fallback if data doesn't match EchoPayload structure exactly
				fmt.Printf("\n%s[INCOMING EVENT]%s Type: %s%s%s, Data: %+v\n", colorGreen, colorReset, colorBold, out.Type, colorReset, out.Data)
			default:
				fmt.Printf("\n%s[INCOMING EVENT]%s Type: %s%s%s, Data: %+v\n", colorGreen, colorReset, colorBold, out.Type, colorReset, out.Data)
			}
			if *interactiveFlag {
				fmt.Print("\n" + colorCyan + "Send chat message -> " + colorReset)
			}
		}
	}()

	// 3. Send initial ping event
	sendChatMessage(c, "Initial handshake message from test client!")

	// 4. Handle input or keep alive
	if *interactiveFlag {
		fmt.Printf("%s[INTERACTIVE MODE]%s Type any message below and press Enter to transmit to the server cluster.\n", colorCyan, colorReset)
		fmt.Printf("Press %sCtrl+C%s to exit gracefully.\n\n", colorBold, colorReset)

		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print(colorCyan + "Send chat message -> " + colorReset)

		// Set up goroutine for stdin processing so we can interrupt it cleanly via channel selection
		inputChan := make(chan string)
		go func() {
			for scanner.Scan() {
				inputChan <- scanner.Text()
			}
		}()

		for {
			select {
			case text := <-inputChan:
				trimmed := strings.TrimSpace(text)
				if trimmed == "" {
					fmt.Print(colorCyan + "Send chat message -> " + colorReset)
					continue
				}
				if strings.ToLower(trimmed) == "/exit" || strings.ToLower(trimmed) == "/quit" {
					gracefulClose(c)
					return
				}
				if err := sendChatMessage(c, trimmed); err != nil {
					fmt.Printf("%s[SEND ERROR] Failed to send message: %v%s\n", colorRed, err, colorReset)
				}
			case <-interrupt:
				fmt.Printf("\n%s[SHUTDOWN]%s Ctrl+C received, terminating session...%s\n", colorYellow, colorBold, colorReset)
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
					fmt.Printf("%s[SEND ERROR] Failed to send message: %v%s\n", colorRed, err, colorReset)
					return
				}
				if count >= 5 {
					fmt.Printf("\n%s[FINISHED]%s Completed 5 message transmissions. Closing connection.%s\n", colorGreen, colorBold, colorReset)
					gracefulClose(c)
					return
				}
			case <-interrupt:
				fmt.Printf("\n%s[SHUTDOWN]%s Ctrl+C received, terminating session...%s\n", colorYellow, colorBold, colorReset)
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

	fmt.Printf("%s[SENDING]%s chat_message: %s%s%s\n", colorYellow, colorReset, colorBold, content, colorReset)
	return c.WriteMessage(websocket.TextMessage, payload)
}

// gracefulClose sends a WebSocket close control frame and waits briefly for server confirmation.
func gracefulClose(c *websocket.Conn) {
	fmt.Printf("%s[CLOSING]%s Sending close frame to cluster...%s\n", colorYellow, colorBold, colorReset)
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

	fmt.Printf("%s[PROBING]%s Performing pre-flight health check on %s...\n", colorPurple, colorReset, baseURL)

	// 1. Probe health
	healthResp, err := client.Get(baseURL + "/health")
	if err != nil {
		fmt.Printf("%s  [WARNING]%s Health endpoint unreachable: %v\n", colorYellow, colorReset, err)
	} else {
		defer healthResp.Body.Close()
		body, _ := io.ReadAll(healthResp.Body)
		fmt.Printf("  %sHealth Status:%s %d %s (Body: %s)\n", colorBold, colorReset, healthResp.StatusCode, http.StatusText(healthResp.StatusCode), strings.TrimSpace(string(body)))
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
		fmt.Printf("%s  [WARNING]%s Stats endpoint unreachable: %v\n", colorYellow, colorReset, err)
	} else {
		defer statsResp.Body.Close()
		body, _ := io.ReadAll(statsResp.Body)
		fmt.Printf("  %sServer Stats :%s %s\n", colorBold, colorReset, strings.TrimSpace(string(body)))
	}
}
