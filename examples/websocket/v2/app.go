package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	memorybus "github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
	redisbus "github.com/thanhbvha/go-common/v2/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
	"github.com/thanhbvha/go-common/v2/websocket/core"
	bridge "github.com/thanhbvha/go-common/v2/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
	natsrouting "github.com/thanhbvha/go-common/v2/websocket/routing/nats"
	redisrouting "github.com/thanhbvha/go-common/v2/websocket/routing/redis"
)

type application struct {
	settings  settings
	redis     *goredis.Client
	nats      *gonats.Conn
	delivery  routing.DeliveryTransport
	gateway   *routing.Gateway
	router    *routing.Router
	manager   *core.Manager
	bus       bus.Bus
	bridge    *bridge.Bridge
	intake    *commandIntake
	stopping  atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newApplication(ctx context.Context, s settings) (a *application, result error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	a = &application{settings: s}
	defer func() {
		if result != nil {
			result = errors.Join(result, a.close())
			a = nil
		}
	}()
	options, err := goredis.ParseURL(s.redisURL)
	if err != nil {
		return a, errors.New("invalid WS_V2_REDIS_URL")
	}
	options.ContextTimeoutEnabled = true
	options.DialTimeout = 3 * time.Second
	options.ReadTimeout = 3 * time.Second
	options.WriteTimeout = 3 * time.Second
	a.redis = goredis.NewClient(options)
	if err = a.redis.Ping(ctx).Err(); err != nil {
		return a, errors.New("Redis startup check failed")
	}
	var registryOptions []redisrouting.RegistryOption
	if s.roomNodeIndex {
		registryOptions = append(registryOptions, redisrouting.WithRoomNodeIndex())
	}
	registry := redisrouting.NewRegistry(a.redis, s.namespace, registryOptions...)
	switch s.delivery {
	case "redis":
		a.delivery = redisrouting.NewTransport(a.redis, s.namespace)
	case "nats":
		a.nats, err = gonats.Connect(s.natsURL, gonats.Timeout(3*time.Second), gonats.Name("websocket-v2-example"))
		if err != nil {
			return a, errors.New("NATS startup check failed")
		}
		a.delivery = natsrouting.NewTransport(a.nats, s.namespace)
	}
	a.gateway, err = routing.NewGateway(routing.GatewayConfig{Registry: registry, Transport: a.delivery, RegistryConcurrency: s.registryConcurrency, LeaseTTL: s.leaseTTL, RenewInterval: s.renewInterval, DedupeCapacity: s.dedupeCapacity, MaxPayloadBytes: 16 << 10})
	if err != nil {
		return a, err
	}
	if err = a.gateway.Start(ctx); err != nil {
		return a, errors.New("gateway startup failed")
	}
	a.router, err = routing.NewRouter(routing.RouterConfig{Registry: registry, Transport: a.delivery, Timeout: 3 * time.Second, MaxPayloadBytes: 16 << 10})
	if err != nil {
		return a, err
	}
	var driver transport.Transport
	var notificationOptions []bus.EventOption
	commandOptions := []bus.TaskOption{bus.WithConcurrency(4), bus.WithPrefetch(8), bus.WithMaxPayloadBytes(32 << 10)}
	if s.business == "memory" {
		driver = memorybus.New()
	} else {
		driver = redisbus.New(a.redis)
		notificationOptions = []bus.EventOption{redisbus.WithEventLayoutTargeted(), redisbus.WithBatchSize(64), redisbus.WithBlock(25 * time.Millisecond)}
		commandOptions = append(commandOptions, redisbus.WithBatchSize(64), redisbus.WithBlock(25*time.Millisecond))
	}
	busConfig := bus.DefaultConfig()
	busConfig.Namespace = s.namespace + "-business"
	busConfig.Concurrency = 4
	busConfig.Prefetch = 8
	busConfig.MaxPayloadBytes = 32 << 10
	busConfig.ProcessTimeout = 3 * time.Second
	a.bus, err = bus.Open(busConfig, bus.WithTransport(driver))
	if err != nil {
		_ = driver.Close(ctx)
		return a, err
	}
	a.bridge, err = bridge.New(bridge.Config{Bus: a.bus, Router: a.router, CommandOptions: commandOptions, NotificationOptions: notificationOptions})
	if err != nil {
		return a, err
	}
	if err = a.bus.HandleTask("ws.command", a.handleBusiness); err != nil {
		return a, err
	}
	if err = a.bus.Start(ctx); err != nil {
		return a, errors.New("EventBus startup failed")
	}
	config := core.DefaultManagerConfig()
	config.Distributed = a.gateway // Borrowed; closed separately after manager drain.
	a.intake = &commandIntake{next: a.bridge, done: make(chan struct{})}
	config.CommandPublisher = a.intake
	config.EventRegistry = a.events()
	config.Authorizer = policy{stopping: &a.stopping}
	config.Observer = &core.AtomicObserver{}
	config.MaxConnections = 2000
	config.MaxShards = 16
	config.MaxShardConnections = 500
	config.MaxConnectionsPerUser = 4
	config.MaxConnectionsPerAccount = 100
	config.MaxConnectionsPerTenant = 500
	config.MaxRoomsPerConnection = 8
	config.MaxMessageBytes = 8 << 10
	config.SendQueueSize = 64
	config.InboundQueueSize = 256
	config.ExecutorPartitions = 4
	config.ExecutorQueueSize = 64
	config.MaxHandlerGoroutines = 16
	config.MaxPendingHandlers = 8
	config.MessagesPerSecond = 20
	config.MessageBurst = 40
	config.BytesPerSecond = 32 << 10
	config.ByteBurst = 64 << 10
	config.SlowConsumerDropThreshold = 2
	config.DrainTimeout = s.drainTimeout
	config.DisableLifecycleLogging = true
	if err = config.ValidateProduction(); err != nil {
		return a, err
	}
	a.manager = core.NewManagerWithConfig(config)
	return a, nil
}

func (a *application) close() error {
	a.closeOnce.Do(func() {
		a.stopping.Store(true)
		step := func(label string, close func(context.Context) error) error {
			ctx, cancel := context.WithTimeout(context.Background(), a.settings.drainTimeout+time.Second)
			defer cancel()
			if err := close(ctx); err != nil {
				a.closeErr = errors.Join(a.closeErr, errors.New(label+" did not finish cleanly"), err)
				return err
			}
			return nil
		}
		// Quiesce business publishers before closing their socket destinations.
		if a.intake != nil {
			step("command publication drain", a.intake.stop)
		}
		// DrainTopology keeps publication and the notification router available
		// while accepted command handlers publish their final notifications.
		if a.bus != nil && a.manager != nil {
			step("business commands drain", func(ctx context.Context) error { return a.bus.DrainTopology(ctx, "ws.command") })
			step("notification drain", func(ctx context.Context) error { return a.bus.DrainTopology(ctx, "ws.notification") })
		}
		if a.bus != nil {
			step("EventBus drain", a.bus.Drain)
		}
		if a.manager != nil {
			step("WebSocket drain", a.manager.Shutdown)
		}
		if a.gateway != nil {
			if step("gateway close", a.gateway.Close) != nil {
				step("gateway close retry", a.gateway.Close)
			}
		} else if a.delivery != nil {
			step("delivery close", a.delivery.Close)
		}
		if a.nats != nil {
			a.nats.Close()
		}
		if a.redis != nil {
			a.closeErr = errors.Join(a.closeErr, a.redis.Close())
		}
	})
	return a.closeErr
}
