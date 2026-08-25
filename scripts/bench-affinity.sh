#!/usr/bin/env bash
# Runs the same multi-turn workload twice against simulated workers with a
# constrained KV cache: once with prefix-affinity routing, once with plain
# power-of-two-choices. Prints TTFT, throughput and worker prefix-cache hits.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=./bin
export GATEWAY_ADMIN_TOKEN=${GATEWAY_ADMIN_TOKEN:-admin-local-dev}

cleanup() { pkill -x mockworker 2>/dev/null || true; pkill -x gateway 2>/dev/null || true; }
trap cleanup EXIT

for affinity in true false; do
  cleanup; sleep 1
  $BIN/mockworker -addr :8001 -id vllm-1 -models llama3-8b,mistral-7b -prefill-tps 3000 -kv-blocks 1500 >/dev/null 2>&1 &
  $BIN/mockworker -addr :8002 -id vllm-2 -models llama3-8b -prefill-tps 3000 -kv-blocks 1500 >/dev/null 2>&1 &
  $BIN/mockworker -addr :8003 -id ollama-1 -kind ollama -models phi3,gemma2 >/dev/null 2>&1 &
  $BIN/mockworker -addr :8004 -id vllm-3 -models unused -prefill-tps 3000 -kv-blocks 1500 \
    -gateway http://localhost:8080 -advertise-url http://localhost:8004 >/dev/null 2>&1 &
  GATEWAY_PREFIX_AFFINITY=$affinity GATEWAY_CACHE_ENABLED=false $BIN/gateway -config configs/gateway.yaml >/dev/null 2>&1 &
  sleep 5
  echo "=== prefix_affinity=$affinity"
  $BIN/loadgen -key sk-local-dev -admin-token "$GATEWAY_ADMIN_TOKEN" \
    -conversations 96 -turns 4 -concurrency 32 -system-prompts 24
  echo
done
