package exampleconfig

import (
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
