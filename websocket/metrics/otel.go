// Package metrics exports bounded WebSocket metrics through OpenTelemetry. The
// application owns the MeterProvider and exporter (Prometheus bridge, OTLP...).
// Every instrument is observable: values are read from the manager only when
// the exporter collects, so there is no sampling goroutine and no hot-path cost.
// Labels are fixed enums; user, tenant, room and IP values are never labels.
package metrics

import (
	"context"
	"errors"

	"github.com/thanhbvha/go-common/v2/websocket/core"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Sources are the optional components to export. Manager is required; Gateway
// and Router are set only for a Distributed deployment.
type Sources struct {
	Manager *core.Manager
	Gateway *routing.Gateway
	Router  *routing.Router
}

func reason(v string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("reason", v))
}

// Register wires the instruments to meter and returns a function that removes them.
func Register(meter metric.Meter, src Sources) (unregister func() error, err error) {
	if meter == nil || src.Manager == nil {
		return nil, errors.New("websocket/metrics: meter and manager are required")
	}
	var (
		gauges   = map[string]metric.Int64ObservableGauge{}
		counters = map[string]metric.Int64ObservableCounter{}
		all      []metric.Observable
	)
	gauge := func(name, desc string) {
		if err != nil {
			return
		}
		var g metric.Int64ObservableGauge
		if g, err = meter.Int64ObservableGauge(name, metric.WithDescription(desc)); err == nil {
			gauges[name] = g
			all = append(all, g)
		}
	}
	counter := func(name, desc string) {
		if err != nil {
			return
		}
		var c metric.Int64ObservableCounter
		if c, err = meter.Int64ObservableCounter(name, metric.WithDescription(desc)); err == nil {
			counters[name] = c
			all = append(all, c)
		}
	}

	gauge("websocket.connections", "Open connections on this node")
	gauge("websocket.shards", "Active shards")
	gauge("websocket.ready", "1 when the node accepts new connections")
	gauge("websocket.executor.queue_length", "Tasks waiting in the event executor")
	gauge("websocket.handlers.running", "Event handler goroutines currently running")
	gauge("websocket.handlers.quarantined", "Handlers that outlived their deadline (cumulative)")
	counter("websocket.connections.opened", "Connections opened")
	counter("websocket.connections.closed", "Connections closed")
	counter("websocket.messages.inbound", "Inbound messages")
	counter("websocket.messages.outbound", "Outbound messages")
	counter("websocket.bytes.inbound", "Inbound bytes")
	counter("websocket.bytes.outbound", "Outbound bytes")
	counter("websocket.slow_consumers", "Connections dropped for reading too slowly")
	counter("websocket.drops", "Messages dropped, by reason")
	counter("websocket.drain.forced", "Connections whose drain hit the deadline")
	counter("websocket.pubsub.subscription_errors", "Failed broker subscriptions")
	if src.Gateway != nil {
		gauge("websocket.gateway.sessions", "Sessions registered in the gateway")
		gauge("websocket.gateway.rooms", "Rooms tracked by the gateway")
		counter("websocket.gateway.lease_failures", "Failed session-lease renewals")
		counter("websocket.gateway.rejected", "Deliveries the gateway rejected")
	}
	if src.Router != nil {
		counter("websocket.router.attempts", "Router send attempts")
		counter("websocket.router.unknown", "Sends with an unknown outcome")
		counter("websocket.router.stale_retries", "Stale-target re-resolutions")
	}
	if err != nil {
		return nil, err
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := src.Manager.Snapshot()
		g := func(n string, v int64) { o.ObserveInt64(gauges[n], v) }
		c := func(n string, v uint64, opts ...metric.ObserveOption) { o.ObserveInt64(counters[n], int64(v), opts...) }
		ready := int64(0)
		if s.Readiness.Ready {
			ready = 1
		}
		g("websocket.connections", s.Connections)
		g("websocket.shards", int64(s.Shards))
		g("websocket.ready", ready)
		g("websocket.executor.queue_length", int64(s.Executor.QueueLength))
		g("websocket.handlers.running", s.Handlers.Running)
		g("websocket.handlers.quarantined", s.Handlers.Quarantined)
		c("websocket.connections.opened", s.Runtime.ConnectionsOpened)
		c("websocket.connections.closed", s.Runtime.ConnectionsClosed)
		c("websocket.messages.inbound", s.Runtime.InboundMessages)
		c("websocket.messages.outbound", s.Runtime.OutboundMessages)
		c("websocket.bytes.inbound", s.Runtime.InboundBytes)
		c("websocket.bytes.outbound", s.Runtime.OutboundBytes)
		c("websocket.slow_consumers", s.Runtime.SlowConsumers)
		c("websocket.drain.forced", s.Runtime.DrainForced)
		c("websocket.pubsub.subscription_errors", s.SubscriptionErrors)
		c("websocket.drops", s.Runtime.OutboundDropped, reason("outbound_queue"))
		c("websocket.drops", s.Events.Dropped, reason("event_queue"))
		c("websocket.drops", s.Events.Malformed, reason("malformed"))
		c("websocket.drops", s.Executor.Rejected, reason("executor_rejected"))
		c("websocket.drops", s.Handlers.Rejected, reason("handler_capacity"))
		if src.Gateway != nil {
			gs := src.Gateway.Stats()
			g("websocket.gateway.sessions", int64(gs.Sessions))
			g("websocket.gateway.rooms", int64(gs.Rooms))
			c("websocket.gateway.lease_failures", gs.LeaseFailures)
			c("websocket.gateway.rejected", gs.Rejected)
		}
		if src.Router != nil {
			rs := src.Router.Stats()
			c("websocket.router.attempts", rs.Attempts)
			c("websocket.router.unknown", rs.Unknown)
			c("websocket.router.stale_retries", rs.StaleRetries)
		}
		return nil
	}, all...)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}
