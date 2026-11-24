package routing

import (
	"context"
	"testing"
	"time"
)

// slowRenewRegistry takes delay to answer a batch renewal and honours ctx the way
// a real Redis client does: when the deadline passes the call fails.
type slowRenewRegistry struct {
	SessionRegistry
	delay time.Duration
}

func (r *slowRenewRegistry) RenewBatch(ctx context.Context, ls []Location, _ time.Duration) []RenewResult {
	var err error
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		err = ctx.Err()
	}
	results := make([]RenewResult, len(ls))
	for i, l := range ls {
		results[i] = RenewResult{Location: l, Err: err}
	}
	return results
}

func renewBudgetGateway(t *testing.T, delay time.Duration) *Gateway {
	t.Helper()
	g, err := NewGateway(GatewayConfig{
		Registry:      &slowRenewRegistry{delay: delay},
		Transport:     readyTransport{},
		LeaseTTL:      600 * time.Millisecond,
		RenewInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.cancel)
	g.installLocal(&localSession{
		location: Location{TenantID: "t", UserID: "u", SessionID: "s", NodeID: g.NodeID(), Generation: "g"},
		rooms:    map[string]struct{}{},
	})
	return g
}

// A round that outlasts RenewInterval but finishes well inside the lease is
// healthy. Cutting it off at RenewInterval used to renew nothing, so a registry
// that was merely slow cost the node its lease and its readiness.
func TestSlowRenewalRoundWithinLeaseStillExtendsReadiness(t *testing.T) {
	g := renewBudgetGateway(t, 250*time.Millisecond) // 2.5x RenewInterval, half the budget
	if g.Ready() {
		t.Fatal("precondition: gateway must start without a lease")
	}

	g.renew()

	if !g.Ready() {
		t.Fatalf("a slow but successful round was discarded: %+v", g.Stats())
	}
	if g.Stats().LeaseFailures != 0 {
		t.Fatalf("slow round counted as a failure: %+v", g.Stats())
	}
}

// The budget is bounded: a registry that never answers must not hold a round, and
// the registry permit it owns, open indefinitely.
func TestRenewalRoundIsBoundedByLeaseBudget(t *testing.T) {
	g := renewBudgetGateway(t, time.Hour)

	started := time.Now()
	g.renew()
	elapsed := time.Since(started)

	if g.Ready() || g.Stats().LeaseFailures != 1 {
		t.Fatalf("an unanswered round extended the lease: ready=%v %+v", g.Ready(), g.Stats())
	}
	budget := g.renewBudget()
	if elapsed < budget/2 || elapsed > budget+300*time.Millisecond {
		t.Fatalf("round ran %v, want about the %v budget", elapsed, budget)
	}
}

// A longer budget must not slow shutdown: Gateway.Close waits for the renewal
// loop, so cancelling the gateway has to abort a round in flight.
func TestGatewayCancelAbortsRenewalRoundPromptly(t *testing.T) {
	g := renewBudgetGateway(t, time.Hour)
	done := make(chan struct{})
	go func() { g.renew(); close(done) }()
	time.Sleep(30 * time.Millisecond) // let the round reach the registry

	g.cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancelling the gateway did not abort the renewal round")
	}
}
