# Routing and KV cache locality

The cheapest token is one you never prefill. Inference servers such as vLLM
(`--enable-prefix-caching`) keep the attention keys/values of recently seen
prompt prefixes in GPU memory, in fixed-size blocks. A request whose prompt
starts with cached blocks skips their prefill entirely. But the cache is
**per worker**: send the next turn of a conversation to a different replica
and it pays the full prefill again.

So in a fleet the router decides the prefix cache hit rate. Plain
least-loaded or round-robin routing spreads every prefix across every worker:
each worker ends up trying to cache the whole working set, the caches thrash,
and hit rate collapses as the fleet grows.

## The algorithm

`internal/router` makes three decisions in order.

**1. Candidates.** Healthy workers that advertise the model and whose circuit
breaker admits traffic, minus workers that already failed this request.
Workers that would need a cold model load (Ollama `/api/ps`) carry a load
penalty.

**2. Affinity** (when `router.prefix_affinity` is on). Two keys are derived
from the conversation (`router.Keys`):

| Key | Covers | Purpose |
|---|---|---|
| `conv` | every message up to and including the first user message (or `X-Session-ID`) | identifies a conversation: all later turns share it |
| `group` | the leading system messages | identifies a shared prefix: unrelated conversations with the same long system prompt share it |

* **Sticky**: if the conversation was served before and that worker is not
  overloaded, use it again. Its KV cache holds the whole history.
* **Rendezvous**: otherwise rank candidates by weighted rendezvous hash of
  `group` and take the first that is not overloaded. All replicas compute the
  same ranking, so new conversations that share a system prompt meet on the
  same worker, and adding or removing a worker only moves the prefixes it owns.
* **Bounded load**: a worker is overloaded when
  `load + 1 > ceil(load_factor × (total_load + 1) × weight / total_weight)`
  ("consistent hashing with bounded loads"). A hot prefix fills its owner up
  to `load_factor` (default 1.25×) of the average, then spills to the *next
  worker in its hash order*, so the overflow is also cache-friendly and no
  worker becomes a hot spot.

**3. Power of two choices.** With affinity off, without a usable key, or when
every candidate is overloaded: sample two random candidates and take the less
loaded (ties broken by TTFT p99). With slightly stale load information this
avoids the herding of "always pick the minimum", and the expected maximum load
is exponentially better than random choice.

The micro-batcher feeds the router a whole batch at once, sorted by group key,
and each routing decision reserves an in-flight slot immediately. Same-prefix
requests that arrive together therefore land together, and vLLM's continuous
batcher schedules them in the same step where they share the prefix blocks,
until the bound says to spill.

## Measured effect

`scripts/bench-affinity.sh` replays 96 conversations × 4 turns (24 distinct
~1,600-token system prompts, 32 concurrent) against three simulated vLLM
workers whose KV cache holds 1,500 blocks each: enough for a third of the
working set, which is the regime large fleets live in. Same workload, same
workers; only the routing policy changes:

| | Prefix affinity | Least-loaded (P2C) |
|---|---|---|
| Prefix (KV) cache hit rate | **61–81 %** | 18–27 % |
| TTFT p50 | **48 ms** | 1,034 ms |
| Request latency p50 | **1.13 s** | 1.83 s |
| Completion throughput | **799 tok/s** | 576 tok/s |
| Wall time for 384 requests | **17.5 s** | 24.3 s |

TTFT drops ~20× because most requests skip prefill entirely. Throughput gains
are smaller in this run only because the simulated decode is long (48 tokens at
60 tok/s); for prefill-heavy traffic (RAG, long documents, agents with large
tool schemas) prefill time dominates and the gap widens. That is where "KV
cache reuse is the difference between 50 and 500 tok/s per GPU" comes from.

The simulator (`internal/mockworker`) models prefill as
`uncached_prompt_tokens / prefill_tps` over a block-level LRU prefix cache, so
the numbers above come from the routing policy's effect on cache residency,
not from tuned constants. Re-run them with real vLLM workers by pointing
`configs/gateway.yaml` at them; the gateway exports
`gateway_worker_prefix_cache_hit_rate` from vLLM's own metrics.

## Knobs

| Setting | Default | Effect |
|---|---|---|
| `router.prefix_affinity` | `true` | Enable sticky + rendezvous placement |
| `router.prefix_chars` | `4096` | How much of the prompt defines the keys (≈ the reusable prefix) |
| `router.load_factor` | `1.25` | How far above average a worker may go before spilling. Lower: tighter balance, fewer cache hits. Higher: more hits, more skew. |
| `router.affinity_table_size` | `100000` | Conversations remembered per replica (LRU) |
| `X-Session-ID` header | — | Pins a conversation explicitly (e.g. agent sessions whose first message varies) |
