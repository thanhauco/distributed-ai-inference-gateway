package config

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultValidates(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestParseExpandsEnvAndDurations(t *testing.T) {
	t.Setenv("TEST_REDIS", "redis:6379")
	raw := []byte(`
redis:
  addr: ${TEST_REDIS}
admin:
  token: ${TEST_UNSET_TOKEN:-fallback}
batcher:
  min_wait: 2ms
  max_wait: 15ms
models:
  - name: llama3-8b
    aliases: [gpt-4o-mini]
workers:
  - id: w1
    url: http://w1:8000
    kind: vllm
    models: [llama3-8b]
`)
	cfg := Default()
	if err := Parse(raw, cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "redis:6379" {
		t.Fatalf("env not expanded: %q", cfg.Redis.Addr)
	}
	if cfg.Admin.Token != "fallback" {
		t.Fatalf("default not applied: %q", cfg.Admin.Token)
	}
	if cfg.Batcher.MaxWait != 15*time.Millisecond || cfg.Batcher.MinWait != 2*time.Millisecond {
		t.Fatalf("durations not parsed: %+v", cfg.Batcher)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	cfg := Default()
	if err := Parse([]byte("server:\n  adr: :9000\n"), cfg); err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestValidateCatchesErrors(t *testing.T) {
	cfg := Default()
	cfg.RateLimit.Backend = "redis"
	cfg.Models = []ModelConfig{{Name: "a", Aliases: []string{"x"}}, {Name: "b", Aliases: []string{"x"}}}
	cfg.Workers = []WorkerConfig{{ID: "w", URL: "http://w", Kind: "tgi"}, {ID: "w", URL: "http://w", Kind: "vllm"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation errors")
	}
	for _, want := range []string{"requires redis.addr", `alias "x"`, "kind must be", "duplicate worker"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}

func TestApplyEnv(t *testing.T) {
	cfg := Default()
	env := map[string]string{"GATEWAY_ADDR": ":9999", "GATEWAY_PREFIX_AFFINITY": "false", "REDIS_ADDR": "r:1"}
	ApplyEnv(cfg, func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if cfg.Server.Addr != ":9999" || cfg.Router.PrefixAffinity || cfg.Redis.Addr != "r:1" {
		t.Fatalf("env not applied: %+v %+v", cfg.Server, cfg.Router)
	}
}

func TestWatchReloadsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gw.yaml")
	if err := os.WriteFile(path, []byte("server:\n  addr: \":1\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan *Config, 1)
	go Watch(ctx, path, 10*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)), func(c *Config) { got <- c })
	time.Sleep(30 * time.Millisecond)
	if err := os.WriteFile(path, []byte("server:\n  addr: \":2\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-got:
		if c.Server.Addr != ":2" {
			t.Fatalf("got addr %q", c.Server.Addr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not fire")
	}
}
