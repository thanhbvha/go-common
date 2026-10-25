package exampleconfig

import (
	"os"
	"time"

	commonconfig "github.com/thanhbvha/go-common/config"
)

const DefaultPath = "examples/websocket/config.yaml"

type Config struct {
	ServerStack      string        `mapstructure:"server_stack"`
	ServerBroker     string        `mapstructure:"server_broker"`
	ServerAddr       string        `mapstructure:"server_addr"`
	ServerRedis      string        `mapstructure:"server_redis"`
	ServerNATS       string        `mapstructure:"server_nats"`
	ServerTenant     string        `mapstructure:"server_tenant"`
	ClientToken      string        `mapstructure:"client_token"`
	AdminToken       string        `mapstructure:"admin_token"`
	ClientURL        string        `mapstructure:"client_url"`
	ClientUser       string        `mapstructure:"client_user"`
	ClientTenant     string        `mapstructure:"client_tenant"`
	ClientAccount    string        `mapstructure:"client_account"`
	ClientSession    string        `mapstructure:"client_session"`
	ClientMode       string        `mapstructure:"client_mode"`
	ClientRoom       string        `mapstructure:"client_room"`
	ClientMessage    string        `mapstructure:"client_message"`
	ClientTargetUser string        `mapstructure:"client_target_user"`
	ClientProbe      bool          `mapstructure:"client_probe"`
	LoadTarget       string        `mapstructure:"loadtest_target"`
	LoadConnections  int           `mapstructure:"loadtest_connections"`
	LoadMessages     int           `mapstructure:"loadtest_messages"`
	LoadHold         time.Duration `mapstructure:"loadtest_hold"`
	LoadSlowReaders  int           `mapstructure:"loadtest_slow_readers"`
	LoadEvent        string        `mapstructure:"loadtest_event"`
	LoadScenario     string        `mapstructure:"loadtest_scenario"`
	LoadReconnects   int           `mapstructure:"loadtest_reconnects"`
	LoadChurnDelay   time.Duration `mapstructure:"loadtest_churn_delay"`
	LoadOutput       string        `mapstructure:"loadtest_output"`
}

func Default() Config {
	return Config{
		ServerStack:      "nethttp",
		ServerBroker:     "standalone",
		ServerAddr:       ":8080",
		ServerRedis:      "127.0.0.1:6379",
		ServerNATS:       "nats://127.0.0.1:4222",
		ServerTenant:     "demo",
		ClientURL:        "ws://127.0.0.1:8080/ws",
		ClientUser:       "alice",
		ClientTenant:     "demo",
		ClientAccount:    "member",
		ClientMode:       "interactive",
		ClientRoom:       "demo:lobby",
		ClientMessage:    "hello from websocket client",
		ClientTargetUser: "alice",
		ClientProbe:      true,
		LoadTarget:       "ws://127.0.0.1:8080/ws",
		LoadConnections:  100,
		LoadMessages:     10,
		LoadEvent:        "system.ping",
		LoadScenario:     "message",
		LoadReconnects:   3,
		LoadChurnDelay:   100 * time.Millisecond,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	opts := commonconfig.DefaultOptions()
	opts.Path = path
	opts.EnvPrefix = "WS_EXAMPLE"
	if err := commonconfig.Load(opts, &cfg); err != nil {
		return Config{}, err
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
