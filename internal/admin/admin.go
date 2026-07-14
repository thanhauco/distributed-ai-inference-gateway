// Package admin implements the control plane API: worker registration and
// heartbeats, worker config pull, API key management, and live updates of the
// model catalog and default limits — all without restarting the gateway.
package admin

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/auth"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/catalog"
	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
)

// Handler serves the admin API.
type Handler struct {
	Token             string
	Keys              *auth.Store
	Registry          *registry.Registry
	Catalog           *catalog.Catalog
	HeartbeatInterval time.Duration

	mu          sync.RWMutex
	assignments map[string]Assignment
}

// Assignment is the configuration a worker pulls at startup.
type Assignment struct {
	Models []string          `json:"models"`
	Kind   backend.Kind      `json:"kind,omitempty"`
	Weight float64           `json:"weight,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

// WorkerConfig is the response of the worker config pull endpoint.
type WorkerConfig struct {
	WorkerID          string     `json:"worker_id"`
	Assignment        Assignment `json:"assignment"`
	HeartbeatInterval string     `json:"heartbeat_interval"`
}

// SetAssignment records the models a worker should serve.
func (h *Handler) SetAssignment(id string, a Assignment) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.assignments == nil {
		h.assignments = map[string]Assignment{}
	}
	h.assignments[id] = a
}

func (h *Handler) assignment(id string) (Assignment, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	a, ok := h.assignments[id]
	return a, ok
}

// Register mounts the admin routes on r.
func (h *Handler) Register(r *gin.Engine) {
	g := r.Group("/admin/v1", h.requireToken)
	g.GET("/workers", h.listWorkers)
	g.POST("/workers", h.registerWorker)
	g.POST("/workers/:id/heartbeat", h.heartbeat)
	g.DELETE("/workers/:id", h.removeWorker)
	g.PUT("/workers/:id/assignment", h.putAssignment)

	g.GET("/keys", h.listKeys)
	g.POST("/keys", h.createKey)
	g.DELETE("/keys/:id", h.revokeKey)

	g.GET("/models", h.listModels)
	g.PUT("/models", h.putModels)
	g.GET("/limits", h.getLimits)
	g.PUT("/limits", h.putLimits)

	// Workers pull their configuration at startup with the same token.
	r.GET("/internal/v1/worker-config/:id", h.requireToken, h.workerConfig)
}

func (h *Handler) requireToken(c *gin.Context) {
	tok := auth.BearerToken(c.GetHeader("Authorization"))
	if h.Token == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(h.Token)) != 1 {
		abort(c, http.StatusUnauthorized, "invalid admin token")
		return
	}
	c.Next()
}

func abort(c *gin.Context, status int, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": msg, "type": "admin_error"}})
}

func (h *Handler) listWorkers(c *gin.Context) {
	ws := h.Registry.List()
	out := make([]registry.Status, len(ws))
	for i, w := range ws {
		out[i] = w.Status()
	}
	c.JSON(http.StatusOK, gin.H{"workers": out})
}

func (h *Handler) registerWorker(c *gin.Context) {
	var spec registry.Spec
	if err := c.ShouldBindJSON(&spec); err != nil {
		abort(c, http.StatusBadRequest, err.Error())
		return
	}
	if len(spec.Models) == 0 {
		if a, ok := h.assignment(spec.ID); ok {
			spec.Models = a.Models
		}
	}
	w, err := h.Registry.Upsert(spec, registry.SourceRegistered)
	if err != nil {
		abort(c, http.StatusBadRequest, err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"worker": w.Status(), "heartbeat_interval": h.heartbeatInterval().String()})
}

func (h *Handler) heartbeat(c *gin.Context) {
	var body struct {
		Models []string `json:"models"`
	}
	_ = c.ShouldBindJSON(&body)
	if err := h.Registry.Heartbeat(c.Param("id"), body.Models); err != nil {
		// Tell the worker to re-register (e.g. after a gateway restart).
		abort(c, http.StatusNotFound, err.Error())
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) removeWorker(c *gin.Context) {
	if err := h.Registry.Remove(c.Param("id")); err != nil {
		abort(c, http.StatusNotFound, err.Error())
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) putAssignment(c *gin.Context) {
	var a Assignment
	if err := c.ShouldBindJSON(&a); err != nil || len(a.Models) == 0 {
		abort(c, http.StatusBadRequest, "assignment requires a non-empty models list")
		return
	}
	h.SetAssignment(c.Param("id"), a)
	c.JSON(http.StatusOK, a)
}

func (h *Handler) workerConfig(c *gin.Context) {
	id := c.Param("id")
	a, ok := h.assignment(id)
	if !ok {
		abort(c, http.StatusNotFound, "no assignment for worker "+id)
		return
	}
	c.JSON(http.StatusOK, WorkerConfig{WorkerID: id, Assignment: a, HeartbeatInterval: h.heartbeatInterval().String()})
}

func (h *Handler) heartbeatInterval() time.Duration {
	if h.HeartbeatInterval > 0 {
		return h.HeartbeatInterval
	}
	return 10 * time.Second
}

type keyRequest struct {
	Name           string   `json:"name" binding:"required"`
	RequestsPerMin int64    `json:"requests_per_min"`
	TokensPerMin   int64    `json:"tokens_per_min"`
	Models         []string `json:"models"`
}

func (h *Handler) listKeys(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"keys": h.Keys.List()})
}

func (h *Handler) createKey(c *gin.Context) {
	var req keyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err.Error())
		return
	}
	raw, k, err := h.Keys.Create(auth.Spec{Name: req.Name, RequestsPerMin: req.RequestsPerMin, TokensPerMin: req.TokensPerMin, Models: req.Models})
	if err != nil {
		abort(c, http.StatusInternalServerError, err.Error())
		return
	}
	// The raw key is shown exactly once.
	c.JSON(http.StatusCreated, gin.H{"key": raw, "metadata": k})
}

func (h *Handler) revokeKey(c *gin.Context) {
	if err := h.Keys.Revoke(c.Param("id")); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, auth.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err.Error())
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handler) listModels(c *gin.Context) {
	served := h.Registry.Models()
	sort.Strings(served)
	c.JSON(http.StatusOK, gin.H{"catalog": h.Catalog.Models(), "served": served})
}

func (h *Handler) putModels(c *gin.Context) {
	var body struct {
		Models []catalog.Model `json:"models"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		abort(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.Catalog.SetModels(body.Models); err != nil {
		abort(c, http.StatusBadRequest, err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"catalog": h.Catalog.Models()})
}

func (h *Handler) getLimits(c *gin.Context) { c.JSON(http.StatusOK, h.Catalog.Limits()) }

func (h *Handler) putLimits(c *gin.Context) {
	var l catalog.Limits
	if err := c.ShouldBindJSON(&l); err != nil || l.RequestsPerMin < 0 || l.TokensPerMin < 0 {
		abort(c, http.StatusBadRequest, "limits must be non-negative integers")
		return
	}
	h.Catalog.SetLimits(l)
	c.JSON(http.StatusOK, l)
}
