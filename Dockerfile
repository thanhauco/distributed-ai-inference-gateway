# syntax=docker/dockerfile:1.7
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/mockworker ./cmd/mockworker && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY configs/ /etc/gateway/
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gateway"]
CMD ["-config", "/etc/gateway/gateway.docker.yaml"]
