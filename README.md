# Distributed AI Inference Gateway

An OpenAI-compatible inference gateway in Go that routes `/v1/chat/completions`
across a fleet of vLLM and Ollama GPU workers, with token-level rate limiting,
response caching, micro-batching, KV-cache-aware routing, retries, circuit
breakers and observability.

> Work in progress: the design follows the system diagram in `docs/`.

## Goals

- **KV-cache locality**: route repeat prompt prefixes to the worker that already
  holds them, instead of recomputing prefill on any free GPU.
- **One interface, many models**: OpenAI-compatible API (SSE streaming) in front
  of heterogeneous worker pools (Llama 3, Mistral, Phi, Gemma).
- **Contain partial failures**: timeouts, retries with jitter, circuit breakers
  and failover so one slow worker cannot stall the fleet.

## Development

```bash
make lint test
```
