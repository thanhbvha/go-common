package exampleconfig

import (
	"github.com/thanhbvha/go-common/websocket/core"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLAndEnvironmentOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("server_addr: ':9000'\nclient_token: file-token\nloadtest_connections: 12\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WS_EXAMPLE_SERVER_ADDR", ":9001")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerAddr != ":9001" || cfg.ClientToken != "file-token" || cfg.LoadConnections != 12 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.ServerStack != "nethttp" {
		t.Fatalf("defaults were not preserved: %+v", cfg)
	}
}

func TestManagerLimitsFromEnvironmentWithoutYAMLKeys(t *testing.T) {
	t.Setenv("WS_EXAMPLE_EXECUTOR_PARTITIONS", "7")
	t.Setenv("WS_EXAMPLE_HANDLER_CONCURRENCY", "13")
	t.Setenv("WS_EXAMPLE_LOADTEST_RATE", "2.5")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	manager := core.DefaultManagerConfig()
	cfg.ApplyManagerLimits(&manager)
	if manager.ExecutorPartitions != 7 || manager.MaxHandlerGoroutines != 13 || manager.SendQueueSize != 128 || manager.MaxConnectionsPerAccount != 100 || cfg.LoadRate != 2.5 {
		t.Fatal("environment limits or defaults were lost")
	}
	t.Setenv("WS_EXAMPLE_MAX_CONNECTIONS_PER_ACCOUNT", "0")
	if _, err := Load(""); err == nil {
		t.Fatal("accepted disabled account quota")
	}
}

func TestLoadDotEnvAndLegacyTokenOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("SERVER_ADDR=:9100\nCLIENT_TOKEN=file-token\nCLIENT_MODE=single\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WS_DEMO_TOKEN", "legacy-token")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerAddr != ":9100" || cfg.ClientToken != "legacy-token" || cfg.ClientMode != "single" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestPathFlag(t *testing.T) {
	if got := Path([]string{"-config", "custom.env"}); got != "custom.env" {
		t.Fatalf("Path() = %q", got)
	}
	if got := Path([]string{"-config=custom.yaml"}); got != "custom.yaml" {
		t.Fatalf("Path() = %q", got)
	}
}

func TestLoadProfile(t *testing.T) {
	cfg, err := Load("../../config.loadtest.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LoadConnections != 1000 || cfg.LoadAccounts != 10 || cfg.LoadGroupSize != 5 || cfg.LoadRate != 1 || cfg.MaxConnectionsPerIP != 2000 || cfg.MaxConnectionsPerAccount != 100 || cfg.ExecutorPartitions != core.DefaultManagerConfig().ExecutorPartitions {
		t.Fatal("load profile differs from documented limits")
	}
}
