package orm

import (
	"context"
	"time"
)

// AdmissionStats counts managed operations. One transaction occupies one permit
// for its entire callback and settlement; transaction statements do not reenter.
type AdmissionStats struct {
	InFlight     int
	Queued       int
	Admitted     uint64
	Rejected     uint64
	Waits        uint64
	Canceled     uint64
	WaitDuration time.Duration
}

func (c *Client) AdmissionStats() AdmissionStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.admission
	s.InFlight = c.inFlight
	s.Queued = c.queued
	return s
}
func (c *Client) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }
func (c *Client) release() {
	c.mu.Lock()
	c.inFlight--
	c.notifyLocked()
	if c.closing && c.inFlight == 0 {
		close(c.drained)
	}
	c.mu.Unlock()
}
func (c *Client) acquire(ctx context.Context) (func(), error) {
	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return nil, normalize("admission", err)
	}
	if c.closing {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.inFlight < c.cfg.MaxInFlight && c.queued == 0 {
		c.inFlight++
		c.admission.Admitted++
		c.mu.Unlock()
		return c.release, nil
	}
	if c.queued >= c.cfg.MaxQueued {
		c.admission.Rejected++
		c.mu.Unlock()
		c.observe(ctx, Event{Kind: EventAdmission, Outcome: "rejected"})
		return nil, ErrBusy
	}
	c.queued++
	c.admission.Waits++
	started := time.Now()
	for {
		err := ctx.Err()
		if c.closing {
			err = ErrClosed
		}
		if err != nil {
			c.queued--
			elapsed := time.Since(started)
			c.admission.Canceled++
			c.admission.WaitDuration += elapsed
			c.mu.Unlock()
			c.observe(ctx, Event{Kind: EventAdmission, Outcome: "canceled", WaitDuration: elapsed})
			return nil, normalize("admission", err)
		}
		if c.inFlight < c.cfg.MaxInFlight {
			c.queued--
			c.inFlight++
			c.admission.Admitted++
			elapsed := time.Since(started)
			c.admission.WaitDuration += elapsed
			c.mu.Unlock()
			c.observe(ctx, Event{Kind: EventAdmission, Outcome: "admitted", WaitDuration: elapsed})
			return c.release, nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		c.mu.Lock()
	}
}
