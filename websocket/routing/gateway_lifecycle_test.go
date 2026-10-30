package routing

import (
	"context"
	"errors"
	"testing"
	"time"
)

type renewalRegistry struct {
	SessionRegistry
	results func([]Location) []RenewResult
	rooms   []RoomMembership
	removed []Location
}

func (r *renewalRegistry) RemoveSession(_ context.Context, l Location, _ []string) error {
	r.removed = append(r.removed, l)
	return nil
}

func (r *renewalRegistry) RenewBatch(_ context.Context, ls []Location, _ time.Duration) []RenewResult {
	return r.results(ls)
}
func (r *renewalRegistry) RenewRooms(_ context.Context, rooms []RoomMembership, _ time.Duration) error {
	r.rooms = rooms
	return nil
}

type readyTransport struct{ DeliveryTransport }

func (readyTransport) Ready() bool { return true }

func TestRenewPartialResultsFenceOnlyStaleOwner(t *testing.T) {
	r := &renewalRegistry{}
	g, err := NewGateway(GatewayConfig{Registry: r, Transport: readyTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.cancel()
	closed := false
	for _, id := range []string{"stale", "live"} {
		s := &localSession{location: Location{TenantID: "t", UserID: "u", SessionID: id, NodeID: g.NodeID(), Generation: "g"}, rooms: map[string]struct{}{"room": {}}}
		if id == "stale" {
			s.close = func() {
				closed = true
				if !g.lifecycle.TryLock() {
					t.Error("close callback runs under lifecycle lock")
				} else {
					g.lifecycle.Unlock()
				}
			}
		}
		g.installLocal(s)
	}
	r.results = func(ls []Location) []RenewResult {
		results := make([]RenewResult, len(ls))
		for i, l := range ls {
			results[i].Location = l
			if l.SessionID == "stale" {
				results[i].Err = ErrStale
			}
		}
		return results
	}
	g.renew()
	if !closed || g.Stats().Sessions != 1 || len(r.rooms) != 1 || r.rooms[0].Location.SessionID != "live" || len(r.removed) != 1 || r.removed[0].SessionID != "stale" || !g.Ready() {
		t.Fatalf("partial renewal lost a healthy session: %+v rooms=%+v", g.Stats(), r.rooms)
	}
	previous := g.leaseUntil.Load()
	r.results = func(ls []Location) []RenewResult {
		return []RenewResult{{Location: ls[0], Err: ErrUnavailable}}
	}
	g.renew()
	if g.leaseUntil.Load() != previous || g.Stats().LeaseFailures != 1 {
		t.Fatal("failed renewal extended readiness")
	}
}

func TestRemoveLocalRequiresCurrentOwner(t *testing.T) {
	g, _ := NewGateway(GatewayConfig{Registry: &renewalRegistry{}, Transport: readyTransport{}})
	defer g.cancel()
	old := &localSession{location: Location{TenantID: "t", UserID: "u", SessionID: "s", Generation: "old"}, rooms: map[string]struct{}{"r": {}}}
	current := &localSession{location: old.location, rooms: map[string]struct{}{"r": {}}}
	current.location.Generation = "new"
	g.installLocal(current)
	if g.removeLocal(old) || g.sessions[current.location.Key()] != current || len(g.rooms) != 1 || len(g.users) != 1 {
		t.Fatal("stale cleanup removed the current owner")
	}
	if !g.removeLocal(current) || len(g.sessions)+len(g.rooms)+len(g.users) != 0 {
		t.Fatal("current cleanup leaked indexes")
	}
}

type legacyRenewalRegistry struct{ SessionRegistry }

func (legacyRenewalRegistry) Renew(context.Context, []Location, time.Duration) error {
	return ErrStale
}
func (legacyRenewalRegistry) Resolve(_ context.Context, t, u, s string) ([]Location, error) {
	return []Location{{TenantID: t, UserID: u, SessionID: s, NodeID: "node", Generation: "g"}}, nil
}

func TestLegacyPartialRenewDoesNotExtendLease(t *testing.T) {
	g, _ := NewGateway(GatewayConfig{NodeID: "node", Registry: legacyRenewalRegistry{}, Transport: readyTransport{}})
	defer g.cancel()
	g.installLocal(&localSession{location: Location{TenantID: "t", UserID: "u", SessionID: "s", NodeID: "node", Generation: "g"}})
	g.renew()
	if g.leaseUntil.Load() != 0 || g.Stats().Sessions != 1 || g.Stats().LeaseFailures != 1 {
		t.Fatal("resolve was mistaken for a successful renewal")
	}
}

func TestUnregisterPermanentFailureIsNotRetried(t *testing.T) {
	g, _ := NewGateway(GatewayConfig{Registry: invalidRemovalRegistry{}, Transport: readyTransport{}})
	defer g.cancel()
	if err := g.Remove(context.Background(), Location{}); !errors.Is(err, ErrInvalid) || g.Stats().UnregisterRetries != 0 || g.Stats().UnregisterFailures != 1 {
		t.Fatalf("invalid cleanup retried: %v %+v", err, g.Stats())
	}
}

type invalidRemovalRegistry struct{ SessionRegistry }

func (invalidRemovalRegistry) Remove(context.Context, Location) error { return ErrInvalid }
