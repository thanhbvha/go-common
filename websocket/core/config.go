package core

import (
	"runtime"
	"time"

	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
	"github.com/thanhbvha/go-common/websocket/routing"
)

// PubSubOwnership controls whether manager shutdown also shuts down the bus.
type PubSubOwnership int

const (
	PubSubBorrowed PubSubOwnership = iota
	PubSubOwned
)

// ManagerConfig owns all production-path dependencies and runtime limits.
type ManagerConfig struct {
	// Distributed is a started, borrowed gateway. It replaces legacy cross-node publishing.
	Distributed      *routing.Gateway
	CommandPublisher routing.CommandPublisher
	PubSub           pubsub.Manager
	PubSubOwnership  PubSubOwnership
	EventRegistry    *EventRegistry
	Authorizer       Authorizer
	EventExecutor    *pool.PartitionedExecutor
	Observer         Observer

	MaxConnections            int64
	MaxShards                 int
	MaxShardConnections       int64
	SendQueueSize             int
	InboundQueueSize          int
	BroadcastQueueSize        int
	ExecutorPartitions        int
	ExecutorQueueSize         int
	MaxMessageBytes           int64
	MessagesPerSecond         float64
	MessageBurst              int64
	BytesPerSecond            float64
	ByteBurst                 int64
	MaxPendingHandlers        int32
	MaxHandlerGoroutines      int
	MaxRoomsPerConnection     int
	SlowConsumerDropThreshold int32
	MaxConnectionsPerUser     int64
	MaxConnectionsPerAccount  int64
	MaxConnectionsPerTenant   int64
	DisableLifecycleLogging   bool
	WriteWait                 time.Duration
	PongWait                  time.Duration
	PingPeriod                time.Duration
}

func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		PubSubOwnership:           PubSubBorrowed,
		MaxConnections:            maxTotalConnections,
		MaxShards:                 maxShards,
		MaxShardConnections:       maxConnections,
		SendQueueSize:             defaultSendQueueSize,
		InboundQueueSize:          incomingMessageSize,
		BroadcastQueueSize:        broadcastChannelSize,
		ExecutorPartitions:        runtime.NumCPU(),
		ExecutorQueueSize:         256,
		MaxMessageBytes:           maxMessageSize,
		MessagesPerSecond:         100,
		MessageBurst:              200,
		BytesPerSecond:            64 * 1024,
		ByteBurst:                 128 * 1024,
		MaxPendingHandlers:        maxPendingHandlers,
		MaxHandlerGoroutines:      runtime.NumCPU() * 4,
		MaxRoomsPerConnection:     100,
		SlowConsumerDropThreshold: 1,
		WriteWait:                 writeWait,
		PongWait:                  pongWait,
		PingPeriod:                pingPeriod,
	}
}

func normalizeManagerConfig(config ManagerConfig) (ManagerConfig, bool) {
	defaults := DefaultManagerConfig()
	if config.PubSub == nil {
		config.PubSub = pubsub.NewNoopManager()
		config.PubSubOwnership = PubSubOwned
	}
	if config.EventRegistry == nil {
		config.EventRegistry = NewEventRegistry()
	}
	if config.Authorizer == nil {
		config.Authorizer = allowAuthorizer{}
	}
	if config.Observer == nil {
		config.Observer = &AtomicObserver{}
	}
	if config.MaxConnections <= 0 {
		config.MaxConnections = defaults.MaxConnections
	}
	if config.MaxShards <= 0 {
		config.MaxShards = defaults.MaxShards
	}
	if config.MaxShardConnections <= 0 {
		config.MaxShardConnections = defaults.MaxShardConnections
	}
	if config.SendQueueSize <= 0 {
		config.SendQueueSize = defaults.SendQueueSize
	}
	if config.InboundQueueSize <= 0 {
		config.InboundQueueSize = defaults.InboundQueueSize
	}
	if config.BroadcastQueueSize <= 0 {
		config.BroadcastQueueSize = defaults.BroadcastQueueSize
	}
	if config.ExecutorPartitions <= 0 {
		config.ExecutorPartitions = defaults.ExecutorPartitions
	}
	if config.ExecutorQueueSize <= 0 {
		config.ExecutorQueueSize = defaults.ExecutorQueueSize
	}
	executorOwned := config.EventExecutor == nil
	if executorOwned {
		config.EventExecutor = pool.NewPartitionedExecutor(config.ExecutorPartitions, config.ExecutorQueueSize)
	}
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if config.MessagesPerSecond <= 0 {
		config.MessagesPerSecond = defaults.MessagesPerSecond
	}
	if config.MessageBurst <= 0 {
		config.MessageBurst = defaults.MessageBurst
	}
	if config.BytesPerSecond <= 0 {
		config.BytesPerSecond = defaults.BytesPerSecond
	}
	if config.ByteBurst <= 0 {
		config.ByteBurst = defaults.ByteBurst
	}
	if config.MaxPendingHandlers <= 0 {
		config.MaxPendingHandlers = defaults.MaxPendingHandlers
	}
	if config.MaxHandlerGoroutines <= 0 {
		config.MaxHandlerGoroutines = defaults.MaxHandlerGoroutines
	}
	if config.MaxRoomsPerConnection <= 0 {
		config.MaxRoomsPerConnection = defaults.MaxRoomsPerConnection
	}
	if config.SlowConsumerDropThreshold <= 0 {
		config.SlowConsumerDropThreshold = defaults.SlowConsumerDropThreshold
	}
	if config.WriteWait <= 0 {
		config.WriteWait = defaults.WriteWait
	}
	if config.PongWait <= 0 {
		config.PongWait = defaults.PongWait
	}
	if config.PingPeriod <= 0 {
		config.PingPeriod = defaults.PingPeriod
	}
	return config, executorOwned
}
