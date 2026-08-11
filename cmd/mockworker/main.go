// Command mockworker runs a simulated vLLM or Ollama worker for local
// development, demos and load tests without GPUs.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/mockworker"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
)

func main() {
	var (
		addr       = flag.String("addr", ":8000", "listen address")
		id         = flag.String("id", hostname(), "worker ID")
		kind       = flag.String("kind", "vllm", "protocol to emulate: vllm or ollama")
		models     = flag.String("models", "llama3-8b", "comma-separated models (overridden by the gateway assignment)")
		prefill    = flag.Float64("prefill-tps", 4000, "prefill tokens/s for uncached prompt blocks")
		decode     = flag.Float64("decode-tps", 60, "decode tokens/s per sequence")
		conc       = flag.Int("max-concurrency", 16, "sequences decoded concurrently")
		kvBlocks   = flag.Int("kv-blocks", 4096, "prefix cache capacity in blocks")
		maxTokens  = flag.Int("default-max-tokens", 48, "completion length when max_tokens is unset")
		coldStart  = flag.Duration("cold-start", 2*time.Second, "model load time (ollama)")
		failRate   = flag.Float64("fail-rate", 0, "probability of a 500 before the first token")
		gatewayURL = flag.String("gateway", "", "comma-separated gateway replica URLs to self-register with (empty: no registration)")
		advertise  = flag.String("advertise-url", "", "URL the gateway should use to reach this worker")
	)
	flag.Parse()
	logger := observability.NewLogger(os.Stdout, "info", "json").With("worker", *id)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	modelList := splitList(*models)
	// The registry is per gateway replica (it is rebuilt from probes and
	// heartbeats, never shared), so a worker registers with every replica.
	var agents []*mockworker.Agent
	for _, u := range splitList(*gatewayURL) {
		agents = append(agents, &mockworker.Agent{
			GatewayURL: u, Token: os.Getenv("GATEWAY_ADMIN_TOKEN"), ID: *id,
			AdvertiseURL: *advertise, Kind: *kind, Logger: logger,
		})
	}
	if len(agents) > 0 {
		// Workers pull their model assignment at startup, so adding or moving
		// a model is a config change, not a gateway restart.
		for attempt := 0; ; attempt++ {
			assigned, err := agents[attempt%len(agents)].PullConfig(ctx)
			if err == nil {
				if len(assigned) > 0 {
					modelList = assigned
					logger.Info("pulled model assignment", "models", assigned)
				}
				break
			}
			if attempt == 0 {
				logger.Warn("config pull failed; retrying", "gateway", *gatewayURL, "error", err)
			}
			if attempt >= 30 || ctx.Err() != nil {
				logger.Warn("could not pull config; using flags", "error", err)
				break
			}
			time.Sleep(time.Second)
		}
	}

	srv := mockworker.New(mockworker.Config{
		ID: *id, Kind: *kind, Models: modelList, PrefillTPS: *prefill, DecodeTPS: *decode,
		MaxConcurrency: *conc, PrefixCacheSize: *kvBlocks, DefaultMaxTokens: *maxTokens,
		ColdStart: *coldStart, FailRate: *failRate,
	})
	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		logger.Info("mock worker listening", "addr", *addr, "kind", *kind, "models", modelList)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen failed", "error", err)
			stop()
		}
	}()
	for _, a := range agents {
		a.Models = modelList
		go a.Run(ctx)
	}
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(sctx)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "mock-worker"
	}
	return h
}
