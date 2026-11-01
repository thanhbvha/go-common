package demo

import (
	"crypto/rand"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func configFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestYAMLTokenAndEnvironmentOverride(t *testing.T) {
	t.Setenv("GRPC_DEMO_TOKEN", "")
	token := rand.Text() + rand.Text()
	path := configFile(t, "token: "+token+"\ninsecure: true\n")
	o, err := ParseClient([]string{"-config", path})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := sharedToken(o.Token); err != nil || got != token {
		t.Fatal("YAML token was not loaded")
	}
	override := rand.Text() + rand.Text()
	t.Setenv("GRPC_DEMO_TOKEN", override)
	if got, err := sharedToken(o.Token); err != nil || got != override {
		t.Fatal("shared environment token did not override YAML")
	}
	t.Setenv("GRPC_DEMO_TOKEN", "")
	for _, invalid := range []string{"", "short", token + "\n"} {
		if _, err := sharedToken(invalid); err == nil {
			t.Fatal("invalid YAML token accepted")
		}
	}
}

func TestConfigPrecedence(t *testing.T) {
	path := configFile(t, "addr: 127.0.0.1:6000\ninsecure: true\nreflection: true\nready_delay: 1s\nshutdown_timeout: 20s\notlp_endpoint: localhost:4317\n")
	t.Setenv("GRPC_SERVER_CONFIG", path)
	t.Setenv("GRPC_SERVER_ADDR", "127.0.0.1:7000")
	t.Setenv("GRPC_SERVER_SHUTDOWN_TIMEOUT", "7s")
	// This key is deliberately absent from YAML.
	t.Setenv("GRPC_SERVER_DEMO_ERRORS", "true")
	t.Setenv("GRPC_SERVER_READY_DELAY", "2s")
	loaded, err := ParseServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Address != "127.0.0.1:7000" || loaded.ShutdownTimeout != 7*time.Second || !loaded.Insecure || !loaded.Reflection || !loaded.DemoErrors || loaded.ReadyDelay != 2*time.Second {
		t.Fatal("YAML/environment precedence incorrect")
	}
	cli, err := ParseServer([]string{"-addr=127.0.0.1:8000", "-insecure=false", "-reflection=false", "-ready-delay=0s", "-otlp-endpoint="})
	if err != nil {
		t.Fatal(err)
	}
	if cli.Address != "127.0.0.1:8000" || cli.Insecure || cli.Reflection || cli.ReadyDelay != 0 || cli.OTLPEndpoint != "" || !cli.DemoErrors {
		t.Fatal("explicit CLI overrides (including zero values) incorrect")
	}
	// Explicit -config overrides the environment path; omitted values keep defaults.
	t.Setenv("GRPC_SERVER_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	other := configFile(t, "insecure: true\n")
	if _, err := ParseServer([]string{"-config", other}); err != nil {
		t.Fatal("CLI config path did not override environment path")
	}
	if _, err := ParseServer([]string{"-config="}); err != nil {
		t.Fatal("explicit empty config path did not select env-only mode")
	}
}

func TestConfigEnvironmentOnly(t *testing.T) {
	t.Setenv("GRPC_SERVER_CONFIG", "")
	t.Setenv("GRPC_CLIENT_CONFIG", "")
	t.Setenv("GRPC_SERVER_ADDR", "127.0.0.1:9000")
	t.Setenv("GRPC_SERVER_INSECURE", "true")
	t.Setenv("GRPC_SERVER_REFLECTION", "true")
	t.Setenv("GRPC_SERVER_SHUTDOWN_TIMEOUT", "12s")
	t.Setenv("GRPC_CLIENT_TARGET", "localhost:9000")
	t.Setenv("GRPC_CLIENT_INSECURE", "true")
	t.Setenv("GRPC_CLIENT_TIMEOUT", "250ms")
	t.Setenv("GRPC_CLIENT_SCENARIO", "health")
	t.Setenv("GRPC_CLIENT_CA", "")
	server, err := ParseServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	client, err := ParseClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	if server.Address != "127.0.0.1:9000" || !server.Insecure || !server.Reflection || server.ShutdownTimeout != 12*time.Second {
		t.Fatal("server environment-only config was ignored")
	}
	if client.Target != "localhost:9000" || !client.Insecure || client.Timeout != 250*time.Millisecond || client.Scenario != "health" || client.CA != "" {
		t.Fatal("client environment-only config was ignored")
	}
	if client.Message != "Hello gRPC" || server.ReadyDelay != 0 {
		t.Fatal("unset defaults were lost")
	}
}

func TestClientConfigAndTLSFields(t *testing.T) {
	path := configFile(t, "target: dns:///orders.example.com:443\nscenario: watch\ntimeout: 4s\ncert: client.pem\nkey: client-key.pem\nca: ca.pem\nserver_name: orders.example.com\notlp_endpoint: localhost:4317\notlp_insecure: true\n")
	t.Setenv("GRPC_CLIENT_TIMEOUT", "500ms")
	t.Setenv("GRPC_CLIENT_SERVER_NAME", "override.example.com")
	t.Setenv("GRPC_CLIENT_OTLP_ENDPOINT", "")
	o, err := ParseClient([]string{"-config", path, "-scenario=basic"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Target != "dns:///orders.example.com:443" || o.Cert != "client.pem" || o.Key != "client-key.pem" || o.CA != "ca.pem" || o.ServerName != "override.example.com" || o.Timeout != 500*time.Millisecond || o.Scenario != "basic" || o.OTLPEndpoint != "" || !o.OTLPInsecure || o.Insecure {
		t.Fatal("client TLS/YAML/env configuration incorrect")
	}
}

func TestConfigErrorsAreSanitized(t *testing.T) {
	for _, content := range []string{"insecure: [broken", "timeout: invalid-sensitive-value", "timeout: 0s", "scenario: unsupported"} {
		_, err := ParseClient([]string{"-config", configFile(t, content)})
		if err == nil {
			t.Fatal("invalid config accepted")
		}
		if strings.Contains(err.Error(), "invalid-sensitive-value") {
			t.Fatal("configuration value leaked into error")
		}
	}
	if _, err := ParseServer([]string{"-config", filepath.Join(t.TempDir(), "missing.yaml")}); err == nil {
		t.Fatal("missing explicitly selected file accepted")
	}
	for _, entry := range []struct{ key, value string }{{"GRPC_SERVER_REFLECTION", "invalid-sensitive-value"}, {"GRPC_SERVER_SHUTDOWN_TIMEOUT", "invalid-sensitive-value"}, {"GRPC_SERVER_SHUTDOWN_TIMEOUT", "0s"}} {
		t.Run(entry.key+entry.value, func(t *testing.T) {
			t.Setenv(entry.key, entry.value)
			_, err := ParseServer(nil)
			if err == nil || strings.Contains(err.Error(), "invalid-sensitive-value") {
				t.Fatal("environment validation or sanitization failed")
			}
		})
	}
	t.Setenv("GRPC_SERVER_CONFIG", "missing.yaml")
	if _, err := ParseServer([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatal("help should not require a valid config file")
	}
}

func TestShippedConfigFiles(t *testing.T) {
	for _, name := range []string{"GRPC_SERVER_CONFIG", "GRPC_CLIENT_CONFIG"} {
		t.Setenv(name, "")
	}
	server, err := ParseServer([]string{"-config", "../../server.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := ParseClient([]string{"-config", "../../client.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if !server.Insecure || !client.Insecure || server.Address != "127.0.0.1:50051" || client.Target != "localhost:50051" {
		t.Fatal("shipped loopback YAML configs incorrect")
	}
	// The shipped templates contain no credentials and still require a token.
	t.Setenv("GRPC_DEMO_TOKEN", "")
	if _, err := sharedToken(""); err == nil {
		t.Fatal("file config bypassed authentication requirement")
	}
}
