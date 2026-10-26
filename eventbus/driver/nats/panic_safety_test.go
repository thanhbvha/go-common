package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestDelaySweeperPanicUpdatesHealthAndSignalsWaitGroup(t *testing.T) {
	backend := &natsTransport{
		schedulerStatuses: make(map[string]transport.SchedulerStatus),
		now:               time.Now,
	}
	topo := transport.Topology{Namespace: "panic", JobType: "scheduler"}
	backend.sweeperWG.Add(1)
	backend.runDelaySweeper(context.Background(), topo, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		backend.sweeperWG.Wait()
	}()
	<-done

	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || !status[0].Degraded || status[0].LastErrorOperation != "panic" {
		t.Fatalf("scheduler status = %#v", status)
	}
	if status[0].LastError != transport.ErrRecoveredPanic.Error() {
		t.Fatalf("scheduler error = %q, want %q", status[0].LastError, transport.ErrRecoveredPanic.Error())
	}
}

func TestSubscriberLaneRenewPanicFencesAndStopsHandle(t *testing.T) {
	lease := &subscriberLaneLeaseHandle{
		t: &natsTransport{
			laneLeaseDuration: 3 * time.Millisecond,
			laneRenewalHook: func(context.Context) error {
				panic("secret panic value")
			},
		},
		leaseUntil: time.Now().Add(time.Minute),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}

	go lease.renew()
	select {
	case <-lease.done:
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not stop after panic")
	}
	select {
	case <-lease.stop:
	default:
		t.Fatal("lease handle was not closed after panic")
	}
	if err := lease.checkOwnership(context.Background()); !errors.Is(err, transport.ErrRecoveredPanic) || !errors.Is(err, errSubscriberLaneLeaseLost) {
		t.Fatalf("ownership error = %v", err)
	} else if strings.Contains(err.Error(), "secret panic value") {
		t.Fatalf("ownership error exposed panic value: %v", err)
	}
}
