package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type RouterConfig struct {
	Registry        SessionRegistry
	Transport       DeliveryTransport
	Timeout         time.Duration
	MaxPayloadBytes int
}
type Router struct {
	cfg                        RouterConfig
	attempts, unknown, partial atomic.Uint64
	staleRetries               atomic.Uint64
}

func NewRouter(c RouterConfig) (*Router, error) {
	if c.Registry == nil || c.Transport == nil {
		return nil, ErrInvalid
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = 1 << 20
	}
	return &Router{cfg: c}, nil
}
func (r *Router) SendToUser(ctx context.Context, tenant, user string, payload []byte, id string) (DeliveryResult, error) {
	return r.send(ctx, tenant, user, "", "", payload, id)
}
func (r *Router) SendToSession(ctx context.Context, tenant, user, session string, payload []byte, id string) (DeliveryResult, error) {
	if session == "" {
		return DeliveryResult{}, ErrInvalid
	}
	return r.send(ctx, tenant, user, session, "", payload, id)
}
func (r *Router) SendToRoom(ctx context.Context, tenant, room string, payload []byte, id string) (DeliveryResult, error) {
	if room == "" {
		return DeliveryResult{}, ErrInvalid
	}
	return r.send(ctx, tenant, "", "", room, payload, id)
}
func (r *Router) send(parent context.Context, tenant, user, session, room string, payload []byte, id string) (DeliveryResult, error) {
	if tenant == "" || (user == "" && room == "") || len(payload) > r.cfg.MaxPayloadBytes {
		return DeliveryResult{}, ErrInvalid
	}
	if id == "" {
		id = NewID()
	}
	ctx, cancel := context.WithTimeout(parent, r.cfg.Timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	accepted := map[string]Outcome{}
	latest := map[string]Outcome{}
	roomResults := map[string]DeliveryResult{}
	finish := func() DeliveryResult {
		result := DeliveryResult{}
		if room != "" {
			result.Counts = make(map[Status]int)
			for _, res := range roomResults {
				result.Outcomes = append(result.Outcomes, res.Outcomes...)
				for status, n := range res.Counts {
					result.Counts[status] += n
				}
				if res.Counts == nil {
					for _, o := range res.Outcomes {
						result.Counts[o.Status]++
					}
				}
			}
			result.Offline = len(result.Outcomes) == 0
			if result.Count(Enqueued) > 0 && result.Retryable() {
				r.partial.Add(1)
			}
			return result
		}
		for _, o := range accepted {
			result.Outcomes = append(result.Outcomes, o)
		}
		for key, o := range latest {
			if _, ok := accepted[key]; !ok {
				result.Outcomes = append(result.Outcomes, o)
			}
		}
		result.Offline = len(result.Outcomes) == 0
		if len(accepted) > 0 && result.Retryable() {
			r.partial.Add(1)
		}
		return result
	}
	for attempt := 0; attempt < 3; attempt++ {
		groups := map[string][]Location{}
		if room != "" {
			nodes, err := r.cfg.Registry.ResolveRoom(ctx, tenant, room)
			if err != nil {
				return finish(), err
			}
			for _, node := range nodes {
				groups[node] = nil
			}
		} else {
			locations, err := r.cfg.Registry.Resolve(ctx, tenant, user, session)
			if err != nil {
				return finish(), err
			}
			active := map[string]bool{}
			for _, l := range locations {
				active[l.Key()] = true
				if _, ok := accepted[l.Key()]; !ok {
					groups[l.NodeID] = append(groups[l.NodeID], l)
				}
			}
			for key, o := range latest {
				if !active[key] && (o.Status == Stale || o.Status == NotFound) {
					delete(latest, key)
				}
			}
		}
		if len(groups) == 0 {
			break
		}
		type job struct {
			node      string
			locations []Location
		}
		type reply struct {
			job
			result DeliveryResult
			err    error
		}
		jobs := make(chan job)
		replies := make(chan reply, len(groups))
		workers := len(groups)
		if workers > 32 {
			workers = 32
		}
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					r.attempts.Add(1)
					command := DeliveryCommand{Version: 1, MessageID: id, CorrelationID: NewID(), TenantID: tenant, TargetNode: j.node, Sessions: j.locations, RoomID: room, Payload: payload, ExpiresAt: deadline}
					res, err := r.cfg.Transport.Deliver(ctx, j.node, command)
					replies <- reply{job: j, result: res, err: err}
				}
			}()
		}
		for node, locations := range groups {
			jobs <- job{node, locations}
		}
		close(jobs)
		wg.Wait()
		close(replies)
		stale := false
		for reply := range replies {
			if room != "" {
				if reply.err != nil {
					roomResults[reply.node] = DeliveryResult{Outcomes: []Outcome{{Location: Location{TenantID: tenant, NodeID: reply.node}, Status: Unknown}}}
					r.unknown.Add(1)
				} else {
					roomResults[reply.node] = reply.result
					for _, o := range reply.result.Outcomes {
						if o.Status == Stale || o.Status == NotFound {
							stale = true
						}
					}
				}
				continue
			}
			if reply.err != nil {
				locations := reply.locations
				if len(locations) == 0 {
					locations = []Location{{TenantID: tenant, NodeID: reply.node}}
				}
				for _, l := range locations {
					key := l.Key()
					if l.SessionID == "" {
						key = tuple("node", l.NodeID)
					}
					latest[key] = Outcome{Location: l, Status: Unknown}
					r.unknown.Add(1)
				}
				continue
			}
			for _, o := range reply.result.Outcomes {
				key := o.Location.Key()
				if o.Location.SessionID == "" {
					key = tuple("node", reply.node)
				}
				if o.Status == Enqueued {
					accepted[key] = o
					delete(latest, key)
				} else {
					latest[key] = o
					if o.Status == Stale || o.Status == NotFound {
						stale = true
					}
				}
			}
		}
		if !stale || ctx.Err() != nil {
			break
		}
		if attempt < 2 {
			r.staleRetries.Add(1)
		}
	}
	return finish(), ctx.Err()
}

type RouterStats struct{ Attempts, Unknown, Partial, StaleRetries uint64 }

func (r *Router) Stats() RouterStats {
	return RouterStats{r.attempts.Load(), r.unknown.Load(), r.partial.Load(), r.staleRetries.Load()}
}
