package wshttp

import (
	"context"
	"net/http"
	"testing"

	"github.com/thanhbvha/go-common/v2/websocket/core"
)

func TestLiveStatusIsUnconditional(t *testing.T) {
	status, body := LiveStatus()
	if status != http.StatusOK || body["status"] != "alive" {
		t.Fatalf("status = %d, body = %v", status, body)
	}
}

func TestReadyStatusFollowsManagerReadiness(t *testing.T) {
	manager := newManager(t, core.DefaultManagerConfig())

	status, body := ReadyStatus(manager.Readiness())
	if status != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("running manager: status = %d, body = %v", status, body)
	}

	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, body = ReadyStatus(manager.Readiness())
	if status != http.StatusServiceUnavailable || body["status"] != "not_ready" {
		t.Fatalf("stopped manager: status = %d, body = %v", status, body)
	}
}

// The reason liveness and readiness are separate: a stopped or draining node is
// not ready, but it is still alive and must not be restarted for it.
func TestStoppedManagerIsNotReadyButStillLive(t *testing.T) {
	manager := newManager(t, core.DefaultManagerConfig())
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	live, _ := LiveStatus()
	ready, _ := ReadyStatus(manager.Readiness())
	if live != http.StatusOK || ready != http.StatusServiceUnavailable {
		t.Fatalf("live = %d, ready = %d; want 200 and 503", live, ready)
	}
}
