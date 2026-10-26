package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *natsTransport) subscriberLaneState(topo transport.Topology, durable string) (subscriberLaneState, error) {
	kv, err := t.getKV(t.kvBucketName(topo))
	if err != nil {
		return subscriberLaneState{}, fmt.Errorf("nats: open subscriber lane registry: %w", err)
	}
	entry, err := kv.Get(t.subscriberLaneKey(topo, durable))
	if err != nil {
		return subscriberLaneState{}, fmt.Errorf("nats: read subscriber lane registry: %w", err)
	}
	var state subscriberLaneState
	if err := json.Unmarshal(entry.Value(), &state); err != nil || state.Version != subscriberLaneVersion {
		return subscriberLaneState{}, fmt.Errorf("nats: subscriber retry lane %q has invalid registry state", durable)
	}
	return state, nil
}

func (t *natsTransport) acquireSubscriberLane(ctx context.Context, topo transport.Topology, durable string, create bool, operation string) (*subscriberLaneLeaseHandle, error) {
	kv, err := t.getKV(t.kvBucketName(topo))
	if err != nil {
		return nil, fmt.Errorf("nats: open subscriber lane registry: %w", err)
	}
	key := t.subscriberLaneKey(topo, durable)
	owner := t.instanceID + ":" + newSchedulerID()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, err := kv.Get(key)
		if errors.Is(err, gonats.ErrKeyNotFound) || errors.Is(err, gonats.ErrKeyDeleted) {
			if !create {
				return nil, fmt.Errorf("nats: subscriber retry lane %q is not provisioned", durable)
			}
			state := subscriberLaneState{Version: subscriberLaneVersion, State: "provisioning", Durable: durable, Generation: 1, Owner: owner, LeaseUntil: time.Now().Add(t.subscriberLaneLeaseDuration()).UnixNano()}
			data, marshalErr := json.Marshal(state)
			if marshalErr != nil {
				return nil, marshalErr
			}
			if _, err := kv.Create(key, data); err == nil {
				return newSubscriberLaneLease(t, kv, key, durable, owner, state.LeaseUntil), nil
			} else if !errors.Is(err, gonats.ErrKeyExists) {
				return nil, fmt.Errorf("nats: create subscriber lane registry: %w", err)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("nats: read subscriber lane registry: %w", err)
		}
		var state subscriberLaneState
		if err := json.Unmarshal(entry.Value(), &state); err != nil || state.Version != subscriberLaneVersion {
			return nil, fmt.Errorf("nats: subscriber retry lane %q has invalid registry state", durable)
		}
		if state.State == "quarantined" {
			return nil, fmt.Errorf("%w: subscriber retry lane %q is quarantined", errSubscriberLaneUnavailable, durable)
		}
		if state.State != "active" && state.State != "provisioning" {
			return nil, fmt.Errorf("nats: subscriber retry lane %q has invalid registry state", durable)
		}
		now := time.Now()
		if state.Owner != "" && state.LeaseUntil > now.UnixNano() {
			timer := time.NewTimer(min(subscriberLanePoll, time.Until(time.Unix(0, state.LeaseUntil))))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if !create && operation != "cleanup" && state.State != "active" {
			return nil, fmt.Errorf("%w: subscriber retry lane %q is not active", errSubscriberLaneUnavailable, durable)
		}
		state.Generation++
		state.Owner = owner
		state.LeaseUntil = now.Add(t.subscriberLaneLeaseDuration()).UnixNano()
		if create {
			state.State = "provisioning"
		}
		data, err := json.Marshal(state)
		if err != nil {
			return nil, err
		}
		if _, err := kv.Update(key, data, entry.Revision()); err == nil {
			return newSubscriberLaneLease(t, kv, key, durable, owner, state.LeaseUntil), nil
		} else if !errors.Is(err, gonats.ErrKeyExists) {
			return nil, fmt.Errorf("nats: acquire subscriber lane lease: %w", err)
		}
	}
}

func newSubscriberLaneLease(t *natsTransport, kv gonats.KeyValue, key, durable, owner string, leaseUntil int64) *subscriberLaneLeaseHandle {
	lease := &subscriberLaneLeaseHandle{
		t: t, kv: kv, key: key, durable: durable, owner: owner,
		leaseUntil: time.Unix(0, leaseUntil), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go lease.renew()
	return lease
}

func (l *subscriberLaneLeaseHandle) renew() {
	defer close(l.done)
	transport.Guard(func() {
		l.fence(transport.ErrRecoveredPanic)
		l.closeStop()
		transport.Guard(nil, l.releaseOwnershipAfterPanic)
	}, l.renewLoop)
}

func (l *subscriberLaneLeaseHandle) renewLoop() {
	ticker := time.NewTicker(l.t.subscriberLaneLeaseDuration() / 3)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.mu.Lock()
			deadline := l.leaseUntil
			l.mu.Unlock()
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			if l.t.laneRenewalHook != nil {
				if err := l.t.laneRenewalHook(ctx); err != nil {
					cancel()
					l.fence(fmt.Errorf("nats: renew subscriber lane %q lease: %w", l.durable, err))
					return
				}
			}
			err := l.update(ctx, func(state *subscriberLaneState) {
				state.LeaseUntil = time.Now().Add(l.t.subscriberLaneLeaseDuration()).UnixNano()
			})
			cancel()
			if err != nil {
				l.fence(err)
				return
			}
		}
	}
}

func (l *subscriberLaneLeaseHandle) releaseOwnershipAfterPanic() {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, err := l.kv.Get(l.key)
	if err != nil {
		return
	}
	var state subscriberLaneState
	if json.Unmarshal(entry.Value(), &state) != nil || state.Version != subscriberLaneVersion || state.Owner != l.owner {
		return
	}
	state.Owner = ""
	state.LeaseUntil = 0
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	_, _ = l.kv.Update(l.key, data, entry.Revision())
}

func (l *subscriberLaneLeaseHandle) fence(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lostErr == nil {
		if errors.Is(err, transport.ErrRecoveredPanic) {
			l.lostErr = errors.Join(errSubscriberLaneLeaseLost, transport.ErrRecoveredPanic)
		} else if errors.Is(err, errSubscriberLaneLeaseLost) {
			l.lostErr = err
		} else {
			l.lostErr = fmt.Errorf("%w: %v", errSubscriberLaneLeaseLost, err)
		}
	}
}

func (l *subscriberLaneLeaseHandle) checkOwnership(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _, err := l.readOwnedStateLocked(ctx)
	return err
}

func (l *subscriberLaneLeaseHandle) readOwnedStateLocked(ctx context.Context) (subscriberLaneState, gonats.KeyValueEntry, error) {
	if l.lostErr != nil {
		return subscriberLaneState{}, nil, l.lostErr
	}
	if err := ctx.Err(); err != nil {
		l.lostErr = fmt.Errorf("%w: subscriber lane %q operation context: %v", errSubscriberLaneLeaseLost, l.durable, err)
		return subscriberLaneState{}, nil, l.lostErr
	}
	if !time.Now().Before(l.leaseUntil) {
		l.lostErr = fmt.Errorf("%w: subscriber lane %q lease expired", errSubscriberLaneLeaseLost, l.durable)
		return subscriberLaneState{}, nil, l.lostErr
	}
	entry, err := l.kv.Get(l.key)
	if err != nil {
		l.lostErr = fmt.Errorf("%w: read subscriber lane lease: %v", errSubscriberLaneLeaseLost, err)
		return subscriberLaneState{}, nil, l.lostErr
	}
	var state subscriberLaneState
	if err := json.Unmarshal(entry.Value(), &state); err != nil || state.Version != subscriberLaneVersion || state.Owner != l.owner {
		l.lostErr = fmt.Errorf("%w: subscriber lane %q ownership changed", errSubscriberLaneLeaseLost, l.durable)
		return subscriberLaneState{}, nil, l.lostErr
	}
	if state.LeaseUntil <= time.Now().UnixNano() {
		l.lostErr = fmt.Errorf("%w: subscriber lane %q lease expired", errSubscriberLaneLeaseLost, l.durable)
		return subscriberLaneState{}, nil, l.lostErr
	}
	l.leaseUntil = time.Unix(0, state.LeaseUntil)
	return state, entry, nil
}

func (l *subscriberLaneLeaseHandle) update(ctx context.Context, mutate func(*subscriberLaneState)) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	state, entry, err := l.readOwnedStateLocked(ctx)
	if err != nil {
		return err
	}
	mutate(&state)
	data, err := json.Marshal(state)
	if err != nil {
		l.lostErr = fmt.Errorf("%w: encode subscriber lane lease: %v", errSubscriberLaneLeaseLost, err)
		return l.lostErr
	}
	if _, err := l.kv.Update(l.key, data, entry.Revision()); err != nil {
		l.lostErr = fmt.Errorf("%w: update subscriber lane lease: %v", errSubscriberLaneLeaseLost, err)
		return l.lostErr
	}
	l.leaseUntil = time.Unix(0, state.LeaseUntil)
	return nil
}

func (l *subscriberLaneLeaseHandle) setState(ctx context.Context, state string) error {
	return l.update(ctx, func(current *subscriberLaneState) { current.State = state })
}

func (l *subscriberLaneLeaseHandle) finish(ctx context.Context, state string, ackWait time.Duration, capacity int) error {
	err := l.update(ctx, func(current *subscriberLaneState) {
		current.State = state
		current.Owner = ""
		current.LeaseUntil = 0
		if state == "active" {
			current.AckWaitNanos = int64(ackWait)
			current.MaxAckPending = capacity
		}
	})
	if err == nil {
		l.stopRenewal()
	}
	return err
}

func (l *subscriberLaneLeaseHandle) release(ctx context.Context) {
	l.stopRenewal()
	_ = l.update(ctx, func(state *subscriberLaneState) {
		state.Owner = ""
		state.LeaseUntil = 0
	})
}

func (l *subscriberLaneLeaseHandle) stopRenewal() {
	l.closeStop()
	<-l.done
}

func (l *subscriberLaneLeaseHandle) closeStop() {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
}

func (t *natsTransport) ensureSubscriberLaneIdle(ctx context.Context, sub transport.Subscription, durable string) error {
	main, err := t.js.ConsumerInfo(t.streamName(sub.Topology), durable, gonats.Context(ctx))
	if err != nil && !errors.Is(err, gonats.ErrConsumerNotFound) {
		return fmt.Errorf("nats: inspect subscriber before cleanup: %w", err)
	}
	if err == nil && (main.NumPending != 0 || main.NumAckPending != 0) {
		return fmt.Errorf("nats: subscriber %q still has broadcast work", durable)
	}
	retry, err := t.js.ConsumerInfo(t.retryStreamName(sub.Topology, durable), t.retryConsumerName(sub.Topology, durable), gonats.Context(ctx))
	if err != nil && !errors.Is(err, gonats.ErrConsumerNotFound) && !errors.Is(err, gonats.ErrStreamNotFound) {
		return fmt.Errorf("nats: inspect subscriber retry lane before cleanup: %w", err)
	}
	if err == nil && (retry.NumPending != 0 || retry.NumAckPending != 0) {
		return fmt.Errorf("nats: subscriber %q still has retry work", durable)
	}
	delayed, err := t.subscriberDelayedCount(ctx, sub.Topology, durable)
	if err != nil {
		return err
	}
	if delayed != 0 {
		return fmt.Errorf("nats: subscriber %q still has delayed retry work", durable)
	}
	return nil
}
