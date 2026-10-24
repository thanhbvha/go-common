package httpkit

import (
	"net"
	"net/http"
)

// ClientIP returns the direct peer IP from RemoteAddr. It intentionally does
// not trust forwarding headers because net/http has no built-in trusted-proxy
// configuration.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}
