// Package api exposes the gateway over HTTP: the OpenAI-compatible
// /v1/chat/completions (JSON and SSE) and /v1/models, health endpoints,
// Prometheus metrics and, optionally, the admin API.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/admin"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/gateway"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/observability"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// Options configures the HTTP server.
type Options struct {
	Service      *gateway.Service
	Keys         *auth.Store
	AuthEnabled  bool
	Registry     *registry.Registry
	Catalog      *catalog.Catalog
	Metrics      *observability.Metrics
	Logger       *slog.Logger
	MaxBodyBytes int64
	Admin        *admin.Handler // nil disables the admin API
	Version      string
}

type server struct{ Options }

const keyContextKey = "gateway.api_key"

// NewHandler builds the HTTP handler.
func NewHandler(o Options) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	s := &server{o}
	r := gin.New()
	r.ContextWithFallback = true // c.Done()/c.Err() follow the request context
	r.Use(s.recover, s.observe)

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok", "version": s.Version}) })
	r.GET("/readyz", s.ready)
	if s.Metrics != nil {
		r.GET("/metrics", gin.WrapH(s.Metrics.Handler()))
	}

	v1 := r.Group("/v1", s.authenticate)
	v1.POST("/chat/completions", s.chatCompletions)
	v1.GET("/models", s.models)

	if s.Admin != nil {
		s.Admin.Register(r)
	}
	r.NoRoute(func(c *gin.Context) {
		writeError(c, &gateway.Error{Status: http.StatusNotFound, Type: "invalid_request_error", Code: "not_found", Message: "unknown endpoint " + c.Request.Method + " " + c.Request.URL.Path})
	})
	return r
}

// observe assigns the correlation ID, starts the server span and writes the
// access log.
func (s *server) observe(c *gin.Context) {
	start := time.Now()
	id := c.GetHeader(observability.RequestIDHeader)
	if id == "" || len(id) > 128 {
		id = observability.NewRequestID()
	}
	ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	ctx, span := observability.Tracer().Start(ctx, c.Request.Method+" "+route, trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("http.request.method", c.Request.Method), attribute.String("http.route", route), attribute.String("gateway.request_id", id)))
	defer span.End()
	ctx = observability.WithRequestID(ctx, id)
	c.Request = c.Request.WithContext(ctx)
	c.Header(observability.RequestIDHeader, id)
	if sc := span.SpanContext(); sc.IsValid() {
		c.Header("X-Trace-ID", sc.TraceID().String())
	}

	c.Next()

	status := c.Writer.Status()
	span.SetAttributes(attribute.Int("http.response.status_code", status))
	if status >= 500 {
		span.SetStatus(codes.Error, http.StatusText(status))
	}
	if route == "/metrics" || route == "/healthz" || route == "/readyz" {
		return
	}
	attrs := []any{
		"method", c.Request.Method, "path", c.Request.URL.Path, "status", status,
		"duration_ms", float64(time.Since(start).Microseconds()) / 1000, "bytes", c.Writer.Size(),
		"remote", c.ClientIP(),
	}
	if k, ok := c.Get(keyContextKey); ok {
		attrs = append(attrs, "api_key_id", k.(*auth.Key).ID)
	}
	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelError
	}
	s.Logger.Log(ctx, level, "request", attrs...)
}

func (s *server) recover(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(r)
			}
			s.Logger.ErrorContext(c.Request.Context(), "panic", "panic", fmt.Sprint(r))
			if !c.Writer.Written() {
				writeError(c, &gateway.Error{Status: http.StatusInternalServerError, Type: "server_error", Message: "internal error"})
			}
			c.Abort()
		}
	}()
	c.Next()
}

func (s *server) authenticate(c *gin.Context) {
	if !s.AuthEnabled {
		return
	}
	raw := auth.BearerToken(c.GetHeader("Authorization"))
	if raw == "" {
		raw = c.GetHeader("Api-Key") // Azure-style clients
	}
	k, err := s.Keys.Authenticate(raw)
	if err != nil {
		writeError(c, &gateway.Error{Status: http.StatusUnauthorized, Type: "invalid_request_error", Code: "invalid_api_key", Message: err.Error()})
		c.Abort()
		return
	}
	c.Set(keyContextKey, k)
}

func (s *server) ready(c *gin.Context) {
	if s.Service.Ready() {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "no healthy workers"})
}

func (s *server) chatCompletions(c *gin.Context) {
	if s.MaxBodyBytes > 0 {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, s.MaxBodyBytes)
	}
	var req openai.ChatCompletionRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(c, &gateway.Error{Status: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "request_too_large", Message: "request body too large"})
			return
		}
		var re *openai.RequestError
		msg := "invalid JSON body: " + err.Error()
		if errors.As(err, &re) {
			msg = re.Error()
		}
		writeError(c, &gateway.Error{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_json", Message: msg})
		return
	}
	req.Normalize()
	if err := req.Validate(); err != nil {
		writeError(c, &gateway.Error{Status: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_request", Message: err.Error()})
		return
	}

	call := gateway.Call{Request: &req, SessionID: c.GetHeader("X-Session-ID"), CacheMode: cacheMode(c.Request.Header)}
	if k, ok := c.Get(keyContextKey); ok {
		call.Key = k.(*auth.Key)
	}
	id := "chatcmpl-" + observability.NewRequestID()[:24]
	created := time.Now().Unix()

	if req.Stream {
		sink := &sseSink{c: c, id: id, created: created, includeUsage: req.StreamOptions != nil && req.StreamOptions.IncludeUsage}
		_, err := s.Service.Chat(c.Request.Context(), call, sink)
		if err != nil {
			ge := gateway.AsError(err)
			if sink.started {
				if ge.Status != gateway.StatusClientClosedRequest {
					sink.writeError(ge)
				}
				return
			}
			writeError(c, ge)
		}
		return
	}

	sink := &jsonSink{}
	out, err := s.Service.Chat(c.Request.Context(), call, sink)
	if err != nil {
		writeError(c, gateway.AsError(err))
		return
	}
	setMetaHeaders(c.Writer.Header(), out.Meta)
	c.JSON(http.StatusOK, sink.response(id, created))
}

func cacheMode(h http.Header) gateway.CacheMode {
	switch strings.ToLower(h.Get("X-Gateway-Cache")) {
	case "bypass", "off", "no-store":
		return gateway.CacheBypass
	case "force", "on":
		return gateway.CacheForce
	}
	cc := strings.ToLower(h.Get("Cache-Control"))
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "no-cache") {
		return gateway.CacheBypass
	}
	return gateway.CacheDefault
}

func (s *server) models(c *gin.Context) {
	served := map[string]bool{}
	for _, m := range s.Registry.Models() {
		served[m] = true
	}
	ids := map[string]bool{}
	for m := range served {
		ids[m] = true
	}
	for _, m := range s.Catalog.Models() {
		if served[m.Name] {
			for _, a := range m.Aliases {
				ids[a] = true
			}
		}
	}
	list := openai.ModelList{Object: "list", Data: []openai.Model{}}
	for id := range ids {
		list.Data = append(list.Data, openai.Model{ID: id, Object: "model", OwnedBy: "inference-gateway"})
	}
	sort.Slice(list.Data, func(i, j int) bool { return list.Data[i].ID < list.Data[j].ID })
	c.JSON(http.StatusOK, list)
}

func writeError(c *gin.Context, e *gateway.Error) {
	if e.Status == gateway.StatusClientClosedRequest {
		c.Status(e.Status)
		return
	}
	if e.RateLimit != nil {
		setRateLimitHeaders(c.Writer.Header(), *e.RateLimit)
	}
	if e.RetryAfter > 0 {
		c.Header("Retry-After", retryAfterSeconds(e.RetryAfter))
	}
	c.JSON(e.Status, errorBody(e))
}
