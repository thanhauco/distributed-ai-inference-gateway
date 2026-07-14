package admin

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
)

func setup(t *testing.T) (*gin.Engine, *Handler) {
	gin.SetMode(gin.TestMode)
	cat, _ := catalog.New(nil, catalog.Limits{})
	h := &Handler{
		Token:    "secret",
		Keys:     auth.NewStore(),
		Registry: registry.New(registry.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}),
		Catalog:  cat,
	}
	r := gin.New()
	h.Register(r)
	return r, h
}

func do(r http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestRequiresToken(t *testing.T) {
	r, _ := setup(t)
	if rec := do(r, "GET", "/admin/v1/workers", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := do(r, "GET", "/admin/v1/workers", "wrong", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}
}

func TestWorkerLifecycle(t *testing.T) {
	r, h := setup(t)
	h.SetAssignment("gpu-7", Assignment{Models: []string{"llama3-8b"}})

	rec := do(r, "GET", "/internal/v1/worker-config/gpu-7", "secret", nil)
	var wc WorkerConfig
	_ = json.Unmarshal(rec.Body.Bytes(), &wc)
	if rec.Code != 200 || wc.Assignment.Models[0] != "llama3-8b" {
		t.Fatalf("worker config: %d %s", rec.Code, rec.Body)
	}
	if rec := do(r, "GET", "/internal/v1/worker-config/unknown", "secret", nil); rec.Code != 404 {
		t.Fatalf("unknown worker: %d", rec.Code)
	}

	rec = do(r, "POST", "/admin/v1/workers", "secret", registry.Spec{ID: "gpu-7", URL: "http://gpu-7:8000", Kind: "vllm"})
	if rec.Code != 200 {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	if w := h.Registry.Get("gpu-7"); w == nil || !w.Serves("llama3-8b") || w.Source != registry.SourceRegistered {
		t.Fatal("registration should apply the assignment")
	}
	if rec := do(r, "POST", "/admin/v1/workers/gpu-7/heartbeat", "secret", map[string]any{"models": []string{"llama3-8b", "mistral-7b"}}); rec.Code != 204 {
		t.Fatalf("heartbeat: %d", rec.Code)
	}
	if !h.Registry.Get("gpu-7").Serves("mistral-7b") {
		t.Fatal("heartbeat models not applied")
	}
	if rec := do(r, "DELETE", "/admin/v1/workers/gpu-7", "secret", nil); rec.Code != 204 {
		t.Fatalf("remove: %d", rec.Code)
	}
	if rec := do(r, "POST", "/admin/v1/workers/gpu-7/heartbeat", "secret", nil); rec.Code != 404 {
		t.Fatalf("heartbeat after removal should ask to re-register: %d", rec.Code)
	}
	if rec := do(r, "POST", "/admin/v1/workers", "secret", registry.Spec{ID: "x", URL: "not a url"}); rec.Code != 400 {
		t.Fatalf("bad spec: %d", rec.Code)
	}
}

func TestKeyManagement(t *testing.T) {
	r, h := setup(t)
	rec := do(r, "POST", "/admin/v1/keys", "secret", map[string]any{"name": "team-a", "tokens_per_min": 5000})
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created struct {
		Key      string   `json:"key"`
		Metadata auth.Key `json:"metadata"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	k, err := h.Keys.Authenticate(created.Key)
	if err != nil || k.TokensPerMin != 5000 {
		t.Fatalf("created key unusable: %v", err)
	}
	if rec := do(r, "DELETE", "/admin/v1/keys/"+created.Metadata.ID, "secret", nil); rec.Code != 204 {
		t.Fatalf("revoke: %d", rec.Code)
	}
	if rec := do(r, "DELETE", "/admin/v1/keys/"+created.Metadata.ID, "secret", nil); rec.Code != 404 {
		t.Fatalf("double revoke: %d", rec.Code)
	}
	if rec := do(r, "POST", "/admin/v1/keys", "secret", map[string]any{}); rec.Code != 400 {
		t.Fatalf("missing name: %d", rec.Code)
	}
}

func TestCatalogAndLimits(t *testing.T) {
	r, h := setup(t)
	rec := do(r, "PUT", "/admin/v1/models", "secret", map[string]any{"models": []catalog.Model{{Name: "llama3-70b", Aliases: []string{"gpt-4o"}}}})
	if rec.Code != 200 {
		t.Fatalf("put models: %d %s", rec.Code, rec.Body)
	}
	if m, ok := h.Catalog.Resolve("gpt-4o"); !ok || m.Name != "llama3-70b" {
		t.Fatal("alias not live")
	}
	if rec := do(r, "PUT", "/admin/v1/limits", "secret", catalog.Limits{RequestsPerMin: 10, TokensPerMin: 100}); rec.Code != 200 {
		t.Fatalf("put limits: %d", rec.Code)
	}
	if h.Catalog.Limits().TokensPerMin != 100 {
		t.Fatal("limits not applied")
	}
	if rec := do(r, "PUT", "/admin/v1/limits", "secret", catalog.Limits{TokensPerMin: -1}); rec.Code != 400 {
		t.Fatalf("negative limits: %d", rec.Code)
	}
}
