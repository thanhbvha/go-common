package main

import "testing"

func TestClientURLDoesNotCarryCredentials(t *testing.T) {
	for _, address := range []string{"ws://user:credential@localhost/ws", "ws://localhost/ws?token=fixture", "wss://localhost/ws#fragment", "http://localhost/ws"} {
		if _, err := endpoint(address); err == nil {
			t.Fatal("client accepted an unsafe or non-WebSocket URL")
		}
	}
	if _, err := endpoint("wss://localhost/ws"); err != nil {
		t.Fatal("TLS WebSocket endpoint rejected")
	}
}
