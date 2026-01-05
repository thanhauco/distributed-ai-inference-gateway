GO        ?= go
BIN       := bin
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS   := -s -w -X main.version=$(VERSION)

.PHONY: all build test race cover lint fmt clean

all: lint test build

build: ## Build all commands into ./bin
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

clean:
	rm -rf $(BIN) coverage.out
