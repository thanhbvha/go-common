package logger

import (
	"net"
)

// LogEntry is the standard structure for logging each request across all frameworks
type LogEntry struct {
	Time      string      `json:"time"`
	Method    string      `json:"method"`
	Path      string      `json:"path"`
	Status    int         `json:"status"`
	Latency   string      `json:"latency"`
	IP        string      `json:"ip"`
	ServerIP  string      `json:"server_ip,omitempty"`
	UserAgent string      `json:"user_agent"`
	Request   interface{} `json:"request,omitempty"`
	Response  interface{} `json:"response,omitempty"`
	Error     string      `json:"error,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
}

// localServerIP caches the LAN IP so we don't query network interfaces on every request
var localServerIP = getServerIP()

// safeStringBytes truncates byte slice if it's too long to prevent log bloat and memory leaks
func safeStringBytes(b []byte, maxLen int) string {
	if len(b) == 0 {
		return ""
	}
	if len(b) > maxLen {
		return string(b[:maxLen]) + " ...[truncated]"
	}
	return string(b)
}

// getServerIP retrieves the LAN IP address of the current server
func getServerIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "unknown"
}
