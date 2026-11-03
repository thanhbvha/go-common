package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnvironmentOnlyAndMissingFileFields(t *testing.T) {
	type Nested struct {
		Port int `mapstructure:"port"`
	}
	type App struct {
		Name   string  `mapstructure:"name"`
		Server *Nested `mapstructure:"server"`
	}
	t.Setenv("REVIEW_NAME", "service")
	t.Setenv("REVIEW_SERVER_PORT", "8081")
	opts := DefaultOptions()
	opts.EnvPrefix = "REVIEW"
	var got App
	if err := Load(opts, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "service" || got.Server == nil || got.Server.Port != 8081 {
		t.Fatalf("env fields missing: %+v", got)
	}
	opts.Path = filepath.Join(t.TempDir(), "partial.yaml")
	if err := os.WriteFile(opts.Path, []byte("name: file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Load(opts, &got); err != nil || got.Server.Port != 8081 {
		t.Fatalf("partial file: %+v %v", got, err)
	}
}

func TestWatcherKeepsLastGoodAndCloses(t *testing.T) {
	type App struct {
		Port int `mapstructure:"port"`
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("port: 10\n")
	opts := DefaultOptions()
	opts.Path = path
	opts.Validate = func(v any) error {
		if v.(*App).Port <= 0 {
			return errors.New("port required")
		}
		return nil
	}
	changes := make(chan int, 8)
	failures := make(chan error, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := Watch(ctx, opts, func() any { return &App{} }, func(v any) { changes <- v.(*App).Port }, func(err error) { failures <- err })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if n := <-changes; n != 10 {
		t.Fatal(n)
	}
	write("port: 0\n")
	select {
	case <-failures:
	case <-time.After(3 * time.Second):
		t.Fatal("invalid reload not reported")
	}
	select {
	case n := <-changes:
		t.Fatalf("invalid config published: %d", n)
	default:
	}
	write("port: 20\n")
	select {
	case n := <-changes:
		if n != 20 {
			t.Fatal(n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reload timeout")
	}
	cancel()
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestNestedDotenvAndValidationAtomicity(t *testing.T) {
	type App struct {
		Server struct {
			Port int `mapstructure:"port"`
		} `mapstructure:"server"`
	}
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("SERVER_PORT=8181\n"), 0600); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Path = path
	var got App
	if err := Load(opts, &got); err != nil || got.Server.Port != 8181 {
		t.Fatalf("nested dotenv: %+v %v", got, err)
	}
	opts.Validate = func(any) error { return errors.New("reject") }
	if err := Load(opts, &got); err == nil || got.Server.Port != 8181 {
		t.Fatal("validation changed target", got, err)
	}
}

func FuzzLoadConfiguration(f *testing.F) {
	f.Add([]byte(`{"name":"service","server":{"port":8080}}`))
	f.Add([]byte(`{"server":`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			t.Skip()
		}
		type App struct {
			Name   string `mapstructure:"name"`
			Server struct {
				Port int `mapstructure:"port"`
			} `mapstructure:"server"`
		}
		t.Setenv("CONFIGFUZZ_NAME", "env-service")
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		opts := DefaultOptions()
		opts.Path = path
		opts.EnvPrefix = "CONFIGFUZZ"
		cfg := App{Name: "unchanged"}
		err := Load(opts, &cfg)
		if err != nil && cfg.Name != "unchanged" {
			t.Fatal("failed load partially changed caller")
		}
		if err == nil && cfg.Name != "env-service" {
			t.Fatal("environment binding lost")
		}
	})
}

func TestInitializedDefaultsAndFailedDecodeIsolation(t *testing.T) {
	type DB struct {
		Port   int               `mapstructure:"port"`
		Labels map[string]string `mapstructure:"labels"`
	}
	type App struct {
		Name string   `mapstructure:"name"`
		DB   *DB      `mapstructure:"database"`
		Tags []string `mapstructure:"tags"`
	}
	opts := DefaultOptions()
	opts.EnvPrefix = "DEFAULTTEST"
	opts.Path = filepath.Join(t.TempDir(), "config.yaml")
	initial := func() App {
		return App{Name: "default", DB: &DB{Port: 5432, Labels: map[string]string{"region": "old"}}, Tags: []string{"existing"}}
	}
	cfg := initial()
	if err := os.WriteFile(opts.Path, []byte("name: configured\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Load(opts, &cfg); err != nil || cfg.Name != "configured" || cfg.DB.Port != 5432 || cfg.Tags[0] != "existing" {
		t.Fatal("defaults lost", cfg, err)
	}
	cfg = initial()
	originalDB := cfg.DB
	if err := os.WriteFile(opts.Path, []byte("database:\n  labels:\n    region: changed\ntags: [changed]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEFAULTTEST_DATABASE_PORT", "invalid-port")
	if err := Load(opts, &cfg); err == nil {
		t.Fatal("malformed env accepted")
	}
	if cfg.DB != originalDB || cfg.DB.Labels["region"] != "old" || cfg.Tags[0] != "existing" || cfg.Name != "default" {
		t.Fatalf("failed decode mutated defaults: %+v", cfg)
	}
}

func TestExplicitEmptyEnvironmentOverridesDefault(t *testing.T) {
	type App struct {
		Name string `mapstructure:"name"`
	}
	cfg := App{Name: "default"}
	opts := DefaultOptions()
	opts.EnvPrefix = "EMPTYTEST"
	t.Setenv("EMPTYTEST_NAME", "")
	if err := Load(opts, &cfg); err != nil || cfg.Name != "" {
		t.Fatal("explicit empty env ignored", cfg, err)
	}
}

func TestWatcherSymlinkReplacement(t *testing.T) {
	type App struct {
		Port int `mapstructure:"port"`
	}
	dir := t.TempDir()
	first := filepath.Join(dir, "v1.yaml")
	second := filepath.Join(dir, "v2.yaml")
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(first, []byte("port: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("port: 2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(first, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	opts := DefaultOptions()
	opts.Path = path
	changes := make(chan int, 4)
	watcher, err := Watch(context.Background(), opts, func() any { return &App{} }, func(value any) { changes <- value.(*App).Port }, func(err error) { t.Errorf("watch reload: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	if got := <-changes; got != 1 {
		t.Fatal(got)
	}
	replacement := filepath.Join(dir, "replacement")
	if err = os.Symlink(second, replacement); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-changes:
		if got != 2 {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("symlink swap was not reloaded")
	}
}
