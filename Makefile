GO        ?= go
BIN       := bin
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS   := -s -w -X main.version=$(VERSION)
API_KEY   ?= sk-local-dev
ADMIN     ?= admin-local-dev

.PHONY: all build test race cover lint fmt run run-workers stop loadgen bench-affinity docker compose-up compose-down video clean

all: lint test build

build: ## Build gateway, mockworker and loadgen into ./bin
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/ ./cmd/...

test: ## Unit + end-to-end tests
	$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -coverprofile=coverage.out ./... && $(GO) tool cover -func=coverage.out | tail -1

lint: ## gofmt + go vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	$(GO) vet ./...

fmt:
	gofmt -w .

run-workers: build ## Start simulated vLLM x2 + Ollama on :8001-8003 (background)
	@$(BIN)/mockworker -addr :8001 -id vllm-1 -models llama3-8b,mistral-7b -kv-blocks 1500 > /tmp/vllm-1.log 2>&1 &
	@$(BIN)/mockworker -addr :8002 -id vllm-2 -models llama3-8b -kv-blocks 1500 > /tmp/vllm-2.log 2>&1 &
	@$(BIN)/mockworker -addr :8003 -id ollama-1 -kind ollama -models phi3,gemma2 > /tmp/ollama-1.log 2>&1 &
	@echo "workers up: vllm-1 :8001, vllm-2 :8002, ollama-1 :8003 (logs in /tmp)"

run: build ## Run the gateway in the foreground with configs/gateway.yaml
	$(BIN)/gateway -config configs/gateway.yaml

stop: ## Stop background workers
	-pkill -x mockworker

loadgen: build ## Replay multi-turn conversations against localhost:8080
	$(BIN)/loadgen -key $(API_KEY) -admin-token $(ADMIN) -conversations 96 -turns 4 -concurrency 32 -system-prompts 24

bench-affinity: build ## Compare prefix-affinity routing against plain least-loaded
	@./scripts/bench-affinity.sh

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t inference-gateway:$(VERSION) .

compose-up: ## Full stack: nginx, 2 gateways, workers, Redis, Prometheus, Grafana, Jaeger
	docker compose -f deploy/docker-compose.yml up --build -d

compose-down:
	docker compose -f deploy/docker-compose.yml down

video: ## Re-render the architecture motion-graphics video
	cd video && npm install --no-audit --no-fund && node render.mjs

clean:
	rm -rf $(BIN) coverage.out
