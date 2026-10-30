package exampleconfig

import (
	"errors"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/spf13/viper"
	commonconfig "github.com/thanhbvha/go-common/config"
	"github.com/thanhbvha/go-common/websocket/core"
)

const DefaultPath = "examples/websocket/config.yaml"

type Config struct {
	UpgradeRate              float64       `mapstructure:"upgrade_rate"`
	UpgradeBurst             int64         `mapstructure:"upgrade_burst"`
	MaxConnectionsPerIP      int           `mapstructure:"max_connections_per_ip"`
	ExecutorPartitions       int           `mapstructure:"executor_partitions"`
	ExecutorQueueSize        int           `mapstructure:"executor_queue_size"`
	HandlerConcurrency       int           `mapstructure:"handler_concurrency"`
	SendQueueSize            int           `mapstructure:"send_queue_size"`
	InboundQueueSize         int           `mapstructure:"inbound_queue_size"`
	MaxConnections           int64         `mapstructure:"max_connections"`
	MaxConnectionsPerUser    int64         `mapstructure:"max_connections_per_user"`
	MaxConnectionsPerAccount int64         `mapstructure:"max_connections_per_account"`
	MaxConnectionsPerTenant  int64         `mapstructure:"max_connections_per_tenant"`
	LoadGroupSize            int           `mapstructure:"loadtest_group_size"`
	LoadRate                 float64       `mapstructure:"loadtest_rate"`
	LoadAccounts             int           `mapstructure:"loadtest_accounts"`
	ServerStack              string        `mapstructure:"server_stack"`
	ServerBroker             string        `mapstructure:"server_broker"`
	ServerAddr               string        `mapstructure:"server_addr"`
	ServerRedis              string        `mapstructure:"server_redis"`
	ServerNATS               string        `mapstructure:"server_nats"`
	ServerTenant             string        `mapstructure:"server_tenant"`
	ClientToken              string        `mapstructure:"client_token"`
	AdminToken               string        `mapstructure:"admin_token"`
	ClientURL                string        `mapstructure:"client_url"`
	ClientUser               string        `mapstructure:"client_user"`
	ClientTenant             string        `mapstructure:"client_tenant"`
	ClientAccount            string        `mapstructure:"client_account"`
	ClientSession            string        `mapstructure:"client_session"`
	ClientMode               string        `mapstructure:"client_mode"`
	ClientRoom               string        `mapstructure:"client_room"`
	ClientMessage            string        `mapstructure:"client_message"`
	ClientTargetUser         string        `mapstructure:"client_target_user"`
	ClientProbe              bool          `mapstructure:"client_probe"`
	LoadTarget               string        `mapstructure:"loadtest_target"`
	LoadConnections          int           `mapstructure:"loadtest_connections"`
	LoadMessages             int           `mapstructure:"loadtest_messages"`
	LoadHold                 time.Duration `mapstructure:"loadtest_hold"`
	LoadSlowReaders          int           `mapstructure:"loadtest_slow_readers"`
	LoadEvent                string        `mapstructure:"loadtest_event"`
	LoadScenario             string        `mapstructure:"loadtest_scenario"`
	LoadReconnects           int           `mapstructure:"loadtest_reconnects"`
	LoadChurnDelay           time.Duration `mapstructure:"loadtest_churn_delay"`
	LoadOutput               string        `mapstructure:"loadtest_output"`
}

func Default() Config {
	return Config{
		UpgradeRate:              100,
		UpgradeBurst:             200,
		MaxConnectionsPerIP:      20,
		ExecutorPartitions:       core.DefaultManagerConfig().ExecutorPartitions,
		ExecutorQueueSize:        256,
		HandlerConcurrency:       64,
		SendQueueSize:            128,
		InboundQueueSize:         1024,
		MaxConnections:           20000,
		MaxConnectionsPerUser:    5,
		MaxConnectionsPerAccount: 100,
		MaxConnectionsPerTenant:  5000,
		LoadGroupSize:            5,
		LoadRate:                 1,
		LoadAccounts:             1,
		ServerStack:              "nethttp",
		ServerBroker:             "standalone",
		ServerAddr:               ":8080",
		ServerRedis:              "127.0.0.1:6379",
		ServerNATS:               "nats://127.0.0.1:4222",
		ServerTenant:             "demo",
		ClientURL:                "ws://127.0.0.1:8080/ws",
		ClientUser:               "alice",
		ClientTenant:             "demo",
		ClientAccount:            "member",
		ClientMode:               "interactive",
		ClientRoom:               "demo:lobby",
		ClientMessage:            "hello from websocket client",
		ClientTargetUser:         "alice",
		ClientProbe:              true,
		LoadTarget:               "ws://127.0.0.1:8080/ws",
		LoadConnections:          100,
		LoadMessages:             10,
		LoadEvent:                "system.ping",
		LoadScenario:             "message",
		LoadReconnects:           3,
		LoadChurnDelay:           100 * time.Millisecond,
	}
}

// ApplyManagerLimits shares the tunable limits between the demo gateways.
func (c Config) ApplyManagerLimits(config *core.ManagerConfig) {
	config.ExecutorPartitions = c.ExecutorPartitions
	config.ExecutorQueueSize = c.ExecutorQueueSize
	config.MaxHandlerGoroutines = c.HandlerConcurrency
	config.SendQueueSize = c.SendQueueSize
	config.InboundQueueSize = c.InboundQueueSize
	config.MaxConnections = c.MaxConnections
	config.MaxConnectionsPerUser = c.MaxConnectionsPerUser
	config.MaxConnectionsPerAccount = c.MaxConnectionsPerAccount
	config.MaxConnectionsPerTenant = c.MaxConnectionsPerTenant
}

func Load(path string) (Config, error) {
	cfg := Default()
	opts := commonconfig.DefaultOptions()
	opts.Path = path
	opts.EnvPrefix = "WS_EXAMPLE"
	if err := commonconfig.Load(opts, &cfg); err != nil {
		return Config{}, err
	}
	// Bind every field so environment-only keys work even when absent from YAML.
	env := viper.New()
	env.SetEnvPrefix("WS_EXAMPLE")
	fields := reflect.TypeOf(cfg)
	for index := 0; index < fields.NumField(); index++ {
		if err := env.BindEnv(fields.Field(index).Tag.Get("mapstructure")); err != nil {
			return Config{}, err
		}
	}
	if err := env.Unmarshal(&cfg); err != nil {
		return Config{}, errors.New("invalid example environment configuration")
	}
	if cfg.ExecutorPartitions <= 0 || cfg.ExecutorQueueSize <= 0 || cfg.HandlerConcurrency <= 0 || cfg.SendQueueSize <= 0 || cfg.InboundQueueSize <= 0 || cfg.MaxConnections <= 0 || cfg.MaxConnectionsPerUser <= 0 || cfg.MaxConnectionsPerAccount <= 0 || cfg.MaxConnectionsPerTenant <= 0 {
		return Config{}, errors.New("manager queue, concurrency and connection limits must be positive")
	}
	if math.IsNaN(cfg.UpgradeRate) || math.IsInf(cfg.UpgradeRate, 0) || cfg.UpgradeRate <= 0 || cfg.UpgradeBurst <= 0 || cfg.MaxConnectionsPerIP <= 0 {
		return Config{}, errors.New("upgrade rate, burst and per-IP connection limit must be positive")
	}
	if token := os.Getenv("WS_DEMO_TOKEN"); token != "" {
		cfg.ClientToken = token
	}
	if token := os.Getenv("WS_ADMIN_TOKEN"); token != "" {
		cfg.AdminToken = token
	}
	return cfg, nil
}

func Path(args []string) string {
	path := os.Getenv("WS_EXAMPLE_CONFIG")
	if path == "" {
		path = DefaultPath
		if _, err := os.Stat(path); err != nil {
			if _, localErr := os.Stat("config.yaml"); localErr == nil {
				path = "config.yaml"
			}
		}
	}
	for index, argument := range args {
		if argument == "-config" && index+1 < len(args) {
			return args[index+1]
		}
		const prefix = "-config="
		if len(argument) > len(prefix) && argument[:len(prefix)] == prefix {
			return argument[len(prefix):]
		}
	}
	return path
}
