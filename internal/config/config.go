// Package config loads and validates the gateway configuration.
//
// Configuration is read from a YAML file, environment variables referenced
// as ${VAR} inside the file are expanded, and a small set of well-known
// environment variables override individual fields so the same file can be
// used across environments.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root gateway configuration.
type Config struct {
	Server        ServerConfig        `yaml:"server" json:"server"`
	Auth          AuthConfig          `yaml:"auth" json:"auth"`
	RateLimit     RateLimitConfig     `yaml:"rate_limit" json:"rate_limit"`
	Cache         CacheConfig         `yaml:"cache" json:"cache"`
	Batcher       BatcherConfig       `yaml:"batcher" json:"batcher"`
	Router        RouterConfig        `yaml:"router" json:"router"`
	Resilience    ResilienceConfig    `yaml:"resilience" json:"resilience"`
	Registry      RegistryConfig      `yaml:"registry" json:"registry"`
	Redis         RedisConfig         `yaml:"redis" json:"redis"`
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`
	Admin         AdminConfig         `yaml:"admin" json:"admin"`
	Models        []ModelConfig       `yaml:"models" json:"models"`
	Workers       []WorkerConfig      `yaml:"workers" json:"workers"`
	// Assignments tell self-registering workers which models to load when
	// they pull their config at startup (keyed by worker ID).
	Assignments map[string]AssignmentConfig `yaml:"assignments" json:"assignments"`
}

// AssignmentConfig is the config a self-registering worker pulls.
type AssignmentConfig struct {
	Kind   string            `yaml:"kind" json:"kind"`
	Models []string          `yaml:"models" json:"models"`
	Weight float64           `yaml:"weight" json:"weight"`
	Labels map[string]string `yaml:"labels" json:"labels"`
}

// ServerConfig controls the public HTTP listener.
type ServerConfig struct {
	Addr              string        `yaml:"addr" json:"addr"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout" json:"read_header_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	MaxBodyBytes      int64         `yaml:"max_body_bytes" json:"max_body_bytes"`
}

// AuthConfig configures API key authentication.
type AuthConfig struct {
	Enabled bool           `yaml:"enabled" json:"enabled"`
	Keys    []APIKeyConfig `yaml:"keys" json:"-"`
}

// APIKeyConfig declares a static API key and its limits.
type APIKeyConfig struct {
	Name           string   `yaml:"name" json:"name"`
	Key            string   `yaml:"key" json:"-"`
	RequestsPerMin int64    `yaml:"requests_per_min" json:"requests_per_min"`
	TokensPerMin   int64    `yaml:"tokens_per_min" json:"tokens_per_min"`
	Models         []string `yaml:"models" json:"models"`
}

// RateLimitConfig configures token-level sliding window rate limiting.
type RateLimitConfig struct {
	Enabled               bool          `yaml:"enabled" json:"enabled"`
	Backend               string        `yaml:"backend" json:"backend"` // memory | redis
	Window                time.Duration `yaml:"window" json:"window"`
	Buckets               int           `yaml:"buckets" json:"buckets"`
	DefaultRequestsPerMin int64         `yaml:"default_requests_per_min" json:"default_requests_per_min"`
	DefaultTokensPerMin   int64         `yaml:"default_tokens_per_min" json:"default_tokens_per_min"`
}

// CacheConfig configures the exact + semantic response cache.
type CacheConfig struct {
	Enabled             bool          `yaml:"enabled" json:"enabled"`
	Backend             string        `yaml:"backend" json:"backend"` // memory | redis
	TTL                 time.Duration `yaml:"ttl" json:"ttl"`
	Semantic            bool          `yaml:"semantic" json:"semantic"`
	SimilarityThreshold float64       `yaml:"similarity_threshold" json:"similarity_threshold"`
	EmbeddingDims       int           `yaml:"embedding_dims" json:"embedding_dims"`
	LSHBits             int           `yaml:"lsh_bits" json:"lsh_bits"`
	MaxBucketEntries    int           `yaml:"max_bucket_entries" json:"max_bucket_entries"`
	MaxTemperature      float64       `yaml:"max_temperature" json:"max_temperature"`
	EmbeddingURL        string        `yaml:"embedding_url" json:"embedding_url"`
	EmbeddingModel      string        `yaml:"embedding_model" json:"embedding_model"`
}

// BatcherConfig configures the dynamic micro-batcher.
type BatcherConfig struct {
	Enabled      bool          `yaml:"enabled" json:"enabled"`
	MinWait      time.Duration `yaml:"min_wait" json:"min_wait"`
	MaxWait      time.Duration `yaml:"max_wait" json:"max_wait"`
	MaxBatchSize int           `yaml:"max_batch_size" json:"max_batch_size"`
	Coalesce     bool          `yaml:"coalesce" json:"coalesce"`
}

// RouterConfig configures worker selection.
type RouterConfig struct {
	PrefixAffinity    bool    `yaml:"prefix_affinity" json:"prefix_affinity"`
	PrefixChars       int     `yaml:"prefix_chars" json:"prefix_chars"`
	LoadFactor        float64 `yaml:"load_factor" json:"load_factor"`
	AffinityTableSize int     `yaml:"affinity_table_size" json:"affinity_table_size"`
}

// ResilienceConfig configures timeouts, retries and circuit breakers.
type ResilienceConfig struct {
	RequestTimeout    time.Duration `yaml:"request_timeout" json:"request_timeout"`
	FirstTokenTimeout time.Duration `yaml:"first_token_timeout" json:"first_token_timeout"`
	MaxAttempts       int           `yaml:"max_attempts" json:"max_attempts"`
	BackoffBase       time.Duration `yaml:"backoff_base" json:"backoff_base"`
	BackoffMax        time.Duration `yaml:"backoff_max" json:"backoff_max"`
	Breaker           BreakerConfig `yaml:"breaker" json:"breaker"`
}

// BreakerConfig configures the per-worker circuit breaker.
type BreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold" json:"failure_threshold"`
	LatencyThreshold time.Duration `yaml:"latency_threshold" json:"latency_threshold"`
	SlowThreshold    int           `yaml:"slow_threshold" json:"slow_threshold"`
	OpenDuration     time.Duration `yaml:"open_duration" json:"open_duration"`
	HalfOpenProbes   int           `yaml:"half_open_probes" json:"half_open_probes"`
}

// RegistryConfig configures health probing of workers.
type RegistryConfig struct {
	ProbeInterval  time.Duration `yaml:"probe_interval" json:"probe_interval"`
	ProbeTimeout   time.Duration `yaml:"probe_timeout" json:"probe_timeout"`
	UnhealthyAfter int           `yaml:"unhealthy_after" json:"unhealthy_after"`
	StaleAfter     time.Duration `yaml:"stale_after" json:"stale_after"`
}

// RedisConfig configures the shared Redis connection.
type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	Password string `yaml:"password" json:"-"`
	DB       int    `yaml:"db" json:"db"`
}

// ObservabilityConfig configures logs, metrics and traces.
type ObservabilityConfig struct {
	LogLevel       string  `yaml:"log_level" json:"log_level"`
	LogFormat      string  `yaml:"log_format" json:"log_format"` // json | text
	MetricsEnabled bool    `yaml:"metrics_enabled" json:"metrics_enabled"`
	TracingEnabled bool    `yaml:"tracing_enabled" json:"tracing_enabled"`
	OTLPEndpoint   string  `yaml:"otlp_endpoint" json:"otlp_endpoint"`
	OTLPInsecure   bool    `yaml:"otlp_insecure" json:"otlp_insecure"`
	ServiceName    string  `yaml:"service_name" json:"service_name"`
	SampleRatio    float64 `yaml:"sample_ratio" json:"sample_ratio"`
}

// AdminConfig configures the control plane API.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Token   string `yaml:"token" json:"-"`
}

// ModelConfig declares a model served by the fleet and the aliases clients
// may use for it.
type ModelConfig struct {
	Name             string   `yaml:"name" json:"name"`
	Aliases          []string `yaml:"aliases" json:"aliases"`
	DefaultMaxTokens int      `yaml:"default_max_tokens" json:"default_max_tokens"`
	ContextWindow    int      `yaml:"context_window" json:"context_window"`
}

// WorkerConfig declares a statically configured worker.
type WorkerConfig struct {
	ID     string            `yaml:"id" json:"id"`
	URL    string            `yaml:"url" json:"url"`
	Kind   string            `yaml:"kind" json:"kind"` // vllm | ollama | openai
	Models []string          `yaml:"models" json:"models"`
	Weight float64           `yaml:"weight" json:"weight"`
	Labels map[string]string `yaml:"labels" json:"labels"`
}

// Default returns a configuration with production-sane defaults.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:              ":8080",
			ReadHeaderTimeout: 10 * time.Second,
			ShutdownTimeout:   30 * time.Second,
			MaxBodyBytes:      4 << 20,
		},
		Auth: AuthConfig{Enabled: true},
		RateLimit: RateLimitConfig{
			Enabled:               true,
			Backend:               "memory",
			Window:                time.Minute,
			Buckets:               60,
			DefaultRequestsPerMin: 600,
			DefaultTokensPerMin:   200_000,
		},
		Cache: CacheConfig{
			Enabled:             true,
			Backend:             "memory",
			TTL:                 10 * time.Minute,
			Semantic:            true,
			SimilarityThreshold: 0.95,
			EmbeddingDims:       256,
			LSHBits:             12,
			MaxBucketEntries:    64,
			MaxTemperature:      0.0,
		},
		Batcher: BatcherConfig{
			Enabled:      true,
			MinWait:      5 * time.Millisecond,
			MaxWait:      20 * time.Millisecond,
			MaxBatchSize: 16,
			Coalesce:     true,
		},
		Router: RouterConfig{
			PrefixAffinity:    true,
			PrefixChars:       4096,
			LoadFactor:        1.25,
			AffinityTableSize: 100_000,
		},
		Resilience: ResilienceConfig{
			RequestTimeout:    120 * time.Second,
			FirstTokenTimeout: 15 * time.Second,
			MaxAttempts:       3,
			BackoffBase:       50 * time.Millisecond,
			BackoffMax:        1 * time.Second,
			Breaker: BreakerConfig{
				FailureThreshold: 5,
				LatencyThreshold: 10 * time.Second,
				SlowThreshold:    3,
				OpenDuration:     15 * time.Second,
				HalfOpenProbes:   1,
			},
		},
		Registry: RegistryConfig{
			ProbeInterval:  5 * time.Second,
			ProbeTimeout:   2 * time.Second,
			UnhealthyAfter: 2,
			StaleAfter:     30 * time.Second,
		},
		Redis: RedisConfig{Addr: ""},
		Observability: ObservabilityConfig{
			LogLevel:       "info",
			LogFormat:      "json",
			MetricsEnabled: true,
			TracingEnabled: false,
			OTLPEndpoint:   "localhost:4318",
			OTLPInsecure:   true,
			ServiceName:    "inference-gateway",
			SampleRatio:    1.0,
		},
		Admin: AdminConfig{Enabled: false},
	}
}

// Load reads the configuration at path. An empty path returns defaults with
// environment overrides applied.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read %s: %w", path, err)
		}
		if err := Parse(raw, cfg); err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}
	ApplyEnv(cfg, os.LookupEnv)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse decodes YAML into cfg after expanding ${VAR} references.
func Parse(raw []byte, cfg *Config) error {
	expanded := os.Expand(string(raw), expandVar)
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// expandVar resolves ${VAR} and ${VAR:-default}.
func expandVar(ref string) string {
	name, def, hasDef := strings.Cut(ref, ":-")
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	if hasDef {
		return def
	}
	return ""
}

// ApplyEnv overrides fields from well-known environment variables.
func ApplyEnv(cfg *Config, lookup func(string) (string, bool)) {
	str := func(name string, dst *string) {
		if v, ok := lookup(name); ok && v != "" {
			*dst = v
		}
	}
	boolean := func(name string, dst *bool) {
		if v, ok := lookup(name); ok && v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				*dst = b
			}
		}
	}
	str("GATEWAY_ADDR", &cfg.Server.Addr)
	str("GATEWAY_LOG_LEVEL", &cfg.Observability.LogLevel)
	str("GATEWAY_ADMIN_TOKEN", &cfg.Admin.Token)
	str("REDIS_ADDR", &cfg.Redis.Addr)
	str("REDIS_PASSWORD", &cfg.Redis.Password)
	str("OTEL_EXPORTER_OTLP_ENDPOINT", &cfg.Observability.OTLPEndpoint)
	str("OTEL_SERVICE_NAME", &cfg.Observability.ServiceName)
	boolean("GATEWAY_TRACING_ENABLED", &cfg.Observability.TracingEnabled)
	boolean("GATEWAY_AUTH_ENABLED", &cfg.Auth.Enabled)
	boolean("GATEWAY_PREFIX_AFFINITY", &cfg.Router.PrefixAffinity)
	boolean("GATEWAY_BATCHER_ENABLED", &cfg.Batcher.Enabled)
	boolean("GATEWAY_CACHE_ENABLED", &cfg.Cache.Enabled)
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Server.Addr == "" {
		add("server.addr is required")
	}
	if c.RateLimit.Enabled {
		if c.RateLimit.Window <= 0 {
			add("rate_limit.window must be > 0")
		}
		if c.RateLimit.Buckets <= 0 {
			add("rate_limit.buckets must be > 0")
		}
		if c.RateLimit.Backend != "memory" && c.RateLimit.Backend != "redis" {
			add("rate_limit.backend must be memory or redis, got %q", c.RateLimit.Backend)
		}
		if c.RateLimit.Backend == "redis" && c.Redis.Addr == "" {
			add("rate_limit.backend=redis requires redis.addr")
		}
	}
	if c.Cache.Enabled {
		if c.Cache.Backend != "memory" && c.Cache.Backend != "redis" {
			add("cache.backend must be memory or redis, got %q", c.Cache.Backend)
		}
		if c.Cache.Backend == "redis" && c.Redis.Addr == "" {
			add("cache.backend=redis requires redis.addr")
		}
		if c.Cache.SimilarityThreshold <= 0 || c.Cache.SimilarityThreshold > 1 {
			add("cache.similarity_threshold must be in (0,1]")
		}
		if c.Cache.LSHBits < 1 || c.Cache.LSHBits > 32 {
			add("cache.lsh_bits must be in [1,32]")
		}
	}
	if c.Batcher.Enabled {
		if c.Batcher.MaxWait < c.Batcher.MinWait {
			add("batcher.max_wait must be >= batcher.min_wait")
		}
		if c.Batcher.MaxBatchSize < 1 {
			add("batcher.max_batch_size must be >= 1")
		}
	}
	if c.Router.LoadFactor < 1 {
		add("router.load_factor must be >= 1")
	}
	if c.Resilience.MaxAttempts < 1 {
		add("resilience.max_attempts must be >= 1")
	}
	if c.Admin.Enabled && c.Admin.Token == "" {
		add("admin.token (or GATEWAY_ADMIN_TOKEN) is required when admin is enabled")
	}

	seenModel := map[string]string{}
	for _, m := range c.Models {
		if m.Name == "" {
			add("models[].name is required")
			continue
		}
		for _, n := range append([]string{m.Name}, m.Aliases...) {
			if owner, ok := seenModel[n]; ok {
				add("model name or alias %q declared by both %q and %q", n, owner, m.Name)
			}
			seenModel[n] = m.Name
		}
	}
	seenWorker := map[string]bool{}
	for _, w := range c.Workers {
		switch {
		case w.ID == "":
			add("workers[].id is required")
		case seenWorker[w.ID]:
			add("duplicate worker id %q", w.ID)
		case w.URL == "":
			add("worker %q: url is required", w.ID)
		}
		seenWorker[w.ID] = true
		switch w.Kind {
		case "vllm", "ollama", "openai":
		default:
			add("worker %q: kind must be vllm, ollama or openai, got %q", w.ID, w.Kind)
		}
	}
	for id, a := range c.Assignments {
		if len(a.Models) == 0 {
			add("assignment %q: models is required", id)
		}
	}
	return errors.Join(errs...)
}
