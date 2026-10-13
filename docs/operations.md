# Operations

## Signals

Every request carries one correlation ID (`X-Request-ID`, generated if the
client did not send one). It appears in the JSON access log, in every log line
written while serving the request, as a span attribute, and in the header sent
to the worker. Responses also return `X-Trace-ID`, so a slow request seen by a
client can be opened straight in Jaeger/Tempo.

### Response headers

| Header | Meaning |
|---|---|
| `X-Request-ID` / `X-Trace-ID` | correlation ID / OpenTelemetry trace ID |
| `X-Gateway-Worker` | worker that served the request (`cache` or `coalesced` when no GPU was used) |
| `X-Gateway-Route` | `affinity_sticky`, `affinity_hash`, `affinity_spill`, `p2c`, `single` |
| `X-Gateway-Cache` | `hit_exact`, `hit_semantic`, `miss`, `bypass` |
| `X-Gateway-Attempts` | attempts used (`>1` means a failover happened) |
| `X-Gateway-Coalesced` | `true` when the response was shared with an identical in-flight request |
| `X-Ratelimit-*`, `Retry-After` | OpenAI-compatible rate limit headers |

### Traces

```
POST /v1/chat/completions                       (server span)
└─ gateway.chat                                 model, stream, worker, route, cache, tokens
   ├─ ratelimit.reserve                         tokens, allowed
   ├─ cache.lookup                              result, similarity
   ├─ batcher.wait                              time spent forming the batch
   └─ worker.attempt  (one per attempt)         worker, kind, route, attempt
      ├─ event first_token                      ttft_ms
      └─ event stream_complete                  chunks
```

The worker request carries `traceparent`, so vLLM builds with OpenTelemetry
enabled continue the same trace.

### Metrics (`/metrics`)

| Metric | Type | Labels |
|---|---|---|
| `gateway_requests_total` | counter | model, status, cache |
| `gateway_request_duration_seconds` | histogram | model, stream |
| `gateway_time_to_first_token_seconds` | histogram | model, worker |
| `gateway_tokens_total` | counter | model, type (prompt, completion) |
| `gateway_decode_tokens_per_second` | histogram | model, worker |
| `gateway_in_flight_requests` | gauge | |
| `gateway_cache_lookups_total` | counter | result |
| `gateway_coalesced_requests_total` | counter | |
| `gateway_rate_limited_total` | counter | reason |
| `gateway_batch_size`, `gateway_batch_wait_seconds`, `gateway_batch_target_size` | histogram/gauge | model |
| `gateway_route_decisions_total` | counter | model, reason |
| `gateway_retries_total` | counter | model, cause |
| `gateway_upstream_errors_total` | counter | worker, cause |
| `gateway_worker_healthy`, `_in_flight`, `_queue_depth`, `_running` | gauge | worker |
| `gateway_worker_gpu_utilization`, `_kv_cache_usage`, `_prefix_cache_hit_rate` | gauge | worker |
| `gateway_breaker_state` (0 closed, 1 half-open, 2 open), `gateway_breaker_transitions_total` | gauge/counter | worker |

`deploy/grafana/dashboards/gateway.json` charts all of them: request rate, TTFT
and latency percentiles, tokens/s, cache hit rate, queue depth, GPU and KV
usage, prefix-cache hit rate per worker, routing reasons, batch sizes, 429s,
breakers and retries.

### Suggested alerts

| Alert | Expression (sketch) |
|---|---|
| TTFT p99 regression | `histogram_quantile(0.99, sum by (le) (rate(gateway_time_to_first_token_seconds_bucket[5m]))) > 5` |
| Breaker open | `max(gateway_breaker_state) == 2` for 5m |
| No healthy worker for a model | `sum(gateway_worker_healthy) == 0` |
| Queueing on workers | `max(gateway_worker_queue_depth) > 32` for 10m: scale the pool |
| Prefix cache collapse | `avg(gateway_worker_prefix_cache_hit_rate) < 0.2`: check `router.prefix_affinity`, `load_factor`, KV memory |

## Runbook

**p99 TTFT spikes while p50 is fine.** Look at `gateway_worker_queue_depth`
and TTFT by worker. One hot worker usually means a hot prefix: confirm with
`gateway_route_decisions_total{reason="affinity_spill"}`. Lowering
`router.load_factor` (e.g. 1.25 → 1.1) spreads it sooner.

**429s for a tenant that "only sent a few requests".** Limits are on tokens.
The reservation is prompt + `max_tokens`, refunded to actual usage when the
response completes; clients sending a huge `max_tokens` reserve that much
until then. Check `X-Ratelimit-Remaining-Tokens`.

**A worker keeps flapping open/closed.** Breakers trip on consecutive worker
faults or on TTFT above `resilience.breaker.latency_threshold`. A worker that
is merely overloaded will trip on latency; scale the pool or lower
`max_num_seqs` on vLLM rather than raising the threshold.

**Requests fail with 503 `no_healthy_workers`.** `GET /admin/v1/workers` shows
each worker's last probe error. Static workers stay unhealthy until a probe
succeeds; registered workers disappear after `registry.stale_after` without a
heartbeat.

**Graceful rollout.** On SIGTERM the gateway stops accepting connections and
drains in-flight streams for `server.shutdown_timeout`. The Kubernetes manifest
pairs that with a `preStop` sleep and `terminationGracePeriodSeconds: 60`.

## Configuration reference

See `configs/gateway.yaml` (local), `configs/gateway.docker.yaml` (Compose) and
`configs/gateway.k8s.yaml` (Kubernetes); the schema with defaults lives in
`internal/config/config.go`, and the whole file is validated at startup and
on every reload. Environment overrides: `GATEWAY_ADDR`,
`GATEWAY_LOG_LEVEL`, `GATEWAY_ADMIN_TOKEN`, `REDIS_ADDR`, `REDIS_PASSWORD`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `GATEWAY_TRACING_ENABLED`,
`GATEWAY_AUTH_ENABLED`, `GATEWAY_PREFIX_AFFINITY`, `GATEWAY_BATCHER_ENABLED`,
`GATEWAY_CACHE_ENABLED`. `${VAR}` and `${VAR:-default}` are expanded inside the
YAML file.
