package core

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/pool"
	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
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

	MaxConnections       int64
	MaxShards            int
	MaxShardConnections  int64
	SendQueueSize        int
	InboundQueueSize     int
	BroadcastQueueSize   int
	ExecutorPartitions   int
	ExecutorQueueSize    int
	MaxMessageBytes      int64
	MessagesPerSecond    float64
	MessageBurst         int64
	BytesPerSecond       float64
	ByteBurst            int64
	MaxPendingHandlers   int32
	MaxHandlerGoroutines int
	// MaxLiveHandlersPerConnection bounds handler goroutines still running for
	// one connection, including ones that outlived their timeout. It stops a
	// few clients from draining MaxHandlerGoroutines with handlers that ignore
	// cancellation.
	MaxLiveHandlersPerConnection int
	MaxRoomsPerConnection        int
	SlowConsumerDropThreshold    int32
	// MaxViolations disconnects (close code 1008) a client after this many
	// consecutive rate-limit or malformed-message violations.
	MaxViolations            int32
	MaxConnectionsPerUser    int64
	MaxConnectionsPerAccount int64
	MaxConnectionsPerTenant  int64
	DisableLifecycleLogging  bool
	// AllowUnauthenticatedBroker lets ValidateProduction accept a Distributed
	// gateway whose transport does not sign broker messages. Set it only when
	// the broker itself is the trust boundary (per-node ACLs plus TLS/mTLS) and
	// no other service can publish to its channels. Default false.
	AllowUnauthenticatedBroker bool
	WriteWait                  time.Duration
	PongWait                   time.Duration
	PingPeriod                 time.Duration
	// DrainTimeout bounds outbound flush across a manager/shard shutdown.
	DrainTimeout time.Duration
}

func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		PubSubOwnership:              PubSubBorrowed,
		MaxConnections:               maxTotalConnections,
		MaxShards:                    maxShards,
		MaxShardConnections:          maxConnections,
		SendQueueSize:                defaultSendQueueSize,
		InboundQueueSize:             incomingMessageSize,
		BroadcastQueueSize:           broadcastChannelSize,
		ExecutorPartitions:           runtime.NumCPU(),
		ExecutorQueueSize:            256,
		MaxMessageBytes:              maxMessageSize,
		MessagesPerSecond:            100,
		MessageBurst:                 200,
		BytesPerSecond:               64 * 1024,
		ByteBurst:                    128 * 1024,
		MaxPendingHandlers:           maxPendingHandlers,
		MaxHandlerGoroutines:         runtime.NumCPU() * 4,
		MaxLiveHandlersPerConnection: 4,
		MaxRoomsPerConnection:        100,
		MaxViolations:                20,
		SlowConsumerDropThreshold:    3, // tolerate a short broadcast burst before disconnecting
		WriteWait:                    writeWait,
		PongWait:                     pongWait,
		PingPeriod:                   pingPeriod,
		DrainTimeout:                 5 * time.Second,
	}
}

// Validate rejects limits that are negative or mutually inconsistent. Zero
// values are valid and select defaults, so it is safe to call on a partially
// filled config. It performs no side effects.
func (config ManagerConfig) Validate() error {
	defaults := DefaultManagerConfig()
	negatives := []struct {
		name  string
		value float64
	}{
		{"MaxConnections", float64(config.MaxConnections)},
		{"MaxShards", float64(config.MaxShards)},
		{"MaxShardConnections", float64(config.MaxShardConnections)},
		{"SendQueueSize", float64(config.SendQueueSize)},
		{"InboundQueueSize", float64(config.InboundQueueSize)},
		{"BroadcastQueueSize", float64(config.BroadcastQueueSize)},
		{"ExecutorPartitions", float64(config.ExecutorPartitions)},
		{"ExecutorQueueSize", float64(config.ExecutorQueueSize)},
		{"MaxMessageBytes", float64(config.MaxMessageBytes)},
		{"MessagesPerSecond", config.MessagesPerSecond},
		{"MessageBurst", float64(config.MessageBurst)},
		{"BytesPerSecond", config.BytesPerSecond},
		{"ByteBurst", float64(config.ByteBurst)},
		{"MaxPendingHandlers", float64(config.MaxPendingHandlers)},
		{"MaxHandlerGoroutines", float64(config.MaxHandlerGoroutines)},
		{"MaxLiveHandlersPerConnection", float64(config.MaxLiveHandlersPerConnection)},
		{"MaxRoomsPerConnection", float64(config.MaxRoomsPerConnection)},
		{"SlowConsumerDropThreshold", float64(config.SlowConsumerDropThreshold)},
		{"MaxConnectionsPerUser", float64(config.MaxConnectionsPerUser)},
		{"MaxConnectionsPerAccount", float64(config.MaxConnectionsPerAccount)},
		{"MaxConnectionsPerTenant", float64(config.MaxConnectionsPerTenant)},
		{"WriteWait", float64(config.WriteWait)},
		{"PongWait", float64(config.PongWait)},
		{"PingPeriod", float64(config.PingPeriod)},
		{"DrainTimeout", float64(config.DrainTimeout)},
	}
	for _, field := range negatives {
		if field.value < 0 {
			return fmt.Errorf("websocket: %s must not be negative", field.name)
		}
	}
	pongWait, pingPeriod := config.PongWait, config.PingPeriod
	if pongWait == 0 {
		pongWait = defaults.PongWait
	}
	if pingPeriod == 0 {
		pingPeriod = defaults.PingPeriod
	}
	if pingPeriod >= pongWait {
		return errors.New("websocket: PingPeriod must be shorter than PongWait")
	}
	maxMessage, byteBurst := config.MaxMessageBytes, config.ByteBurst
	if maxMessage == 0 {
		maxMessage = defaults.MaxMessageBytes
	}
	if byteBurst == 0 {
		byteBurst = defaults.ByteBurst
	}
	if maxMessage > byteBurst {
		return errors.New("websocket: MaxMessageBytes must not exceed ByteBurst, otherwise a maximum-size message can never be accepted")
	}
	return nil
}

// ValidateProduction requires an explicit application authorization policy and
// a supported fan-out topology. Call this before NewManagerWithConfig; trusted
// local constructors retain their permissive defaults. Authentication still
// belongs to the HTTP adapter, and the application policy must enforce
// tenant/room access for verified identities.
//
// Cluster deployments must use Distributed (routing.Gateway + routing.Router).
// The legacy Shard/pubsub fan-out is single-node only, so a real PubSub backend
// is rejected, and rejected next to Distributed where it would be ignored.
func (config ManagerConfig) ValidateProduction() error {
	if err := config.Validate(); err != nil {
		return err
	}
	if config.PubSub != nil {
		if _, noop := config.PubSub.(*pubsub.NoopManager); !noop {
			if config.Distributed != nil {
				return errors.New("websocket: PubSub is ignored when Distributed is set; remove it")
			}
			return errors.New("websocket: legacy PubSub fan-out is single-node only; clusters must use Distributed (routing.Gateway)")
		}
	}
	if config.Distributed != nil && !config.AllowUnauthenticatedBroker && !config.Distributed.RequiresAuthentication() {
		return errors.New("websocket: Distributed delivery must authenticate broker messages (routing.WithMessageAuthenticator with pubsub.AuthRequired), or set AllowUnauthenticatedBroker when broker ACLs and TLS are the trust boundary")
	}
	if config.Authorizer == nil {
		return errors.New("websocket: production requires an explicit Authorizer")
	}
	value := reflect.ValueOf(config.Authorizer)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		if value.IsNil() {
			return errors.New("websocket: production Authorizer must not be typed nil")
		}
	}
	return nil
}

// NewProductionManager validates config with ValidateProduction and only then
// starts a Manager. Prefer it over NewManagerWithConfig in deployed services:
// it cannot be reached with a missing authorizer, a legacy cluster fan-out, or
// an unauthenticated broker, and it never touches process-global defaults.
func NewProductionManager(config ManagerConfig) (*Manager, error) {
	if err := config.ValidateProduction(); err != nil {
		return nil, err
	}
	return NewManagerWithConfig(config), nil
}

func normalizeManagerConfig(config ManagerConfig) (ManagerConfig, bool) {
	defaults := DefaultManagerConfig()
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = defaults.DrainTimeout
	}
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
	if config.MaxLiveHandlersPerConnection <= 0 {
		config.MaxLiveHandlersPerConnection = defaults.MaxLiveHandlersPerConnection
	}
	if config.MaxRoomsPerConnection <= 0 {
		config.MaxRoomsPerConnection = defaults.MaxRoomsPerConnection
	}
	if config.SlowConsumerDropThreshold <= 0 {
		config.SlowConsumerDropThreshold = defaults.SlowConsumerDropThreshold
	}
	if config.MaxViolations <= 0 {
		config.MaxViolations = defaults.MaxViolations
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
