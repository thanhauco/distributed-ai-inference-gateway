// Command gateway runs the distributed AI inference gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/admin"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/api"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/cache"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/config"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/gateway"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/ratelimit"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/resilience"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/router"
)

var version = "dev"

func main() {
	configPath := flag.String("config", os.Getenv("GATEWAY_CONFIG"), "path to the gateway YAML config")
	reloadEvery := flag.Duration("reload-interval", 5*time.Second, "how often to check the config file for changes (0 disables)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(*configPath, *reloadEvery); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func run(configPath string, reloadEvery time.Duration) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	logger := observability.NewLogger(os.Stdout, cfg.Observability.LogLevel, cfg.Observability.LogFormat)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, observability.TracingConfig{
		Enabled:     cfg.Observability.TracingEnabled,
		Endpoint:    cfg.Observability.OTLPEndpoint,
		Insecure:    cfg.Observability.OTLPInsecure,
		ServiceName: cfg.Observability.ServiceName,
		Version:     version,
		SampleRatio: cfg.Observability.SampleRatio,
	})
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

	metrics := observability.NewMetrics()

	// Late-bound hooks: the registry needs them before the service exists.
	var svc *gateway.Service
	var rt *router.Router
	reg := registry.New(registry.Options{
		HTTPClient: &http.Client{Transport: workerTransport()},
		Breaker: resilience.BreakerConfig{
			FailureThreshold: cfg.Resilience.Breaker.FailureThreshold,
			LatencyThreshold: cfg.Resilience.Breaker.LatencyThreshold,
			SlowThreshold:    cfg.Resilience.Breaker.SlowThreshold,
			OpenDuration:     cfg.Resilience.Breaker.OpenDuration,
			HalfOpenProbes:   cfg.Resilience.Breaker.HalfOpenProbes,
		},
		Logger: logger,
		OnBreakerChange: func(w *registry.Worker, from, to resilience.State) {
			if svc != nil {
				svc.ObserveBreaker(w, from, to)
			}
		},
		OnRemove: func(id string) {
			metrics.DeleteWorker(id)
			if rt != nil {
				rt.Forget(id)
			}
		},
	})
	syncStaticWorkers(reg, cfg, logger)

	rt = router.New(reg, router.Config{
		PrefixAffinity:    cfg.Router.PrefixAffinity,
		PrefixChars:       cfg.Router.PrefixChars,
		LoadFactor:        cfg.Router.LoadFactor,
		AffinityTableSize: cfg.Router.AffinityTableSize,
	})

	cat, err := catalog.FromConfig(cfg)
	if err != nil {
		return err
	}
	keys := auth.NewStore()
	keys.ReplaceSource("config", keySpecs(cfg))
	if cfg.Auth.Enabled && len(cfg.Auth.Keys) == 0 && !cfg.Admin.Enabled {
		logger.Warn("auth is enabled but no keys are configured and the admin API is off: every request will be rejected")
	}

	var rdb redis.UniversalClient
	if cfg.Redis.Addr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := rdb.Ping(pctx).Err()
		cancel()
		if err != nil {
			return fmt.Errorf("redis %s: %w", cfg.Redis.Addr, err)
		}
		defer rdb.Close()
	}

	var limiter ratelimit.Limiter
	if cfg.RateLimit.Enabled {
		rlCfg := ratelimit.Config{Window: cfg.RateLimit.Window, Buckets: cfg.RateLimit.Buckets}
		if cfg.RateLimit.Backend == "redis" {
			limiter = ratelimit.NewRedis(rdb, rlCfg)
		} else {
			limiter = ratelimit.NewMemory(rlCfg)
		}
	}

	var respCache *cache.Cache
	if cfg.Cache.Enabled {
		var store cache.Store = cache.NewMemoryStore(50_000)
		if cfg.Cache.Backend == "redis" {
			store = cache.NewRedisStore(rdb)
		}
		var embedder cache.Embedder = cache.HashingEmbedder{Dims: cfg.Cache.EmbeddingDims}
		if cfg.Cache.EmbeddingURL != "" {
			embedder = cache.RemoteEmbedder{URL: cfg.Cache.EmbeddingURL, Model: cfg.Cache.EmbeddingModel, Client: &http.Client{Timeout: 2 * time.Second}}
		}
		respCache = cache.New(store, embedder, cache.Config{
			TTL:                 cfg.Cache.TTL,
			Semantic:            cfg.Cache.Semantic,
			SimilarityThreshold: cfg.Cache.SimilarityThreshold,
			MaxBucketEntries:    cfg.Cache.MaxBucketEntries,
			LSHBits:             cfg.Cache.LSHBits,
			EmbeddingDims:       cfg.Cache.EmbeddingDims,
		})
	}

	svc = gateway.New(gateway.Options{
		Config: cfg, Registry: reg, Router: rt, Catalog: cat,
		Limiter: limiter, Cache: respCache, Metrics: metrics, Logger: logger,
	})

	var adminHandler *admin.Handler
	if cfg.Admin.Enabled {
		adminHandler = &admin.Handler{Token: cfg.Admin.Token, Keys: keys, Registry: reg, Catalog: cat, HeartbeatInterval: cfg.Registry.StaleAfter / 3}
		applyAssignments(adminHandler, cfg)
	}

	prober := &registry.Prober{
		Registry: reg, Interval: cfg.Registry.ProbeInterval, Timeout: cfg.Registry.ProbeTimeout,
		UnhealthyAfter: cfg.Registry.UnhealthyAfter, StaleAfter: cfg.Registry.StaleAfter,
		OnProbe: svc.ObserveProbe,
	}
	go prober.Run(ctx)

	go config.Watch(ctx, configPath, reloadEvery, logger, func(next *config.Config) {
		if err := cat.Replace(catalog.ModelsFromConfig(next.Models), catalog.Limits{
			RequestsPerMin: next.RateLimit.DefaultRequestsPerMin, TokensPerMin: next.RateLimit.DefaultTokensPerMin,
		}); err != nil {
			logger.Warn("catalog reload rejected", "error", err)
		}
		keys.ReplaceSource("config", keySpecs(next))
		syncStaticWorkers(reg, next, logger)
		if adminHandler != nil {
			applyAssignments(adminHandler, next)
		}
	})

	handler := api.NewHandler(api.Options{
		Service: svc, Keys: keys, AuthEnabled: cfg.Auth.Enabled, Registry: reg, Catalog: cat,
		Metrics: metrics, Logger: logger, MaxBodyBytes: cfg.Server.MaxBodyBytes, Admin: adminHandler, Version: version,
	})
	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		// No WriteTimeout: streams legitimately run for minutes; the
		// per-request timeout in the pipeline bounds them instead.
		IdleTimeout: 120 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", "addr", cfg.Server.Addr, "version", version,
			"workers", len(reg.List()), "auth", cfg.Auth.Enabled, "rate_limit", cfg.RateLimit.Backend,
			"cache", cfg.Cache.Enabled, "batcher", cfg.Batcher.Enabled, "prefix_affinity", cfg.Router.PrefixAffinity)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down; draining in-flight streams", "timeout", cfg.Server.ShutdownTimeout)
		sctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			logger.Warn("forced shutdown", "error", err)
		}
	}
	return nil
}

// workerTransport is tuned for many long-lived streams to a few hosts.
func workerTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 1024
	t.MaxIdleConnsPerHost = 256
	t.IdleConnTimeout = 90 * time.Second
	t.ResponseHeaderTimeout = 0 // first-token timeout is enforced per attempt
	return t
}

func keySpecs(cfg *config.Config) map[string]auth.Spec {
	out := make(map[string]auth.Spec, len(cfg.Auth.Keys))
	for _, k := range cfg.Auth.Keys {
		if k.Key == "" {
			continue
		}
		out[k.Key] = auth.Spec{Name: k.Name, RequestsPerMin: k.RequestsPerMin, TokensPerMin: k.TokensPerMin, Models: k.Models}
	}
	return out
}

// syncStaticWorkers makes the registry's static workers match cfg.
func syncStaticWorkers(reg *registry.Registry, cfg *config.Config, logger *slog.Logger) {
	want := map[string]bool{}
	for _, w := range cfg.Workers {
		want[w.ID] = true
		if _, err := reg.Upsert(registry.Spec{
			ID: w.ID, URL: w.URL, Kind: backend.Kind(w.Kind), Models: w.Models, Weight: w.Weight, Labels: w.Labels,
		}, registry.SourceStatic); err != nil {
			logger.Error("invalid worker in config", "worker", w.ID, "error", err)
		}
	}
	for _, w := range reg.List() {
		if w.Source == registry.SourceStatic && !want[w.ID] {
			_ = reg.Remove(w.ID)
		}
	}
}

func applyAssignments(h *admin.Handler, cfg *config.Config) {
	for id, a := range cfg.Assignments {
		h.SetAssignment(id, admin.Assignment{Models: a.Models, Kind: backend.Kind(a.Kind), Weight: a.Weight, Labels: a.Labels})
	}
	for _, w := range cfg.Workers {
		h.SetAssignment(w.ID, admin.Assignment{Models: w.Models, Kind: backend.Kind(w.Kind), Weight: w.Weight, Labels: w.Labels})
	}
}
