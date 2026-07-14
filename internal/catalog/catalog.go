// Package catalog holds the hot-reloadable model catalog (canonical names,
// aliases, per-model defaults) and the default rate limits. It is read on
// every request and replaced atomically by config reloads and the admin API.
package catalog

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/config"
)

// Model is a catalog entry.
type Model struct {
	Name             string   `json:"name"`
	Aliases          []string `json:"aliases,omitempty"`
	DefaultMaxTokens int      `json:"default_max_tokens,omitempty"`
	ContextWindow    int      `json:"context_window,omitempty"`
}

// Limits are the per-key defaults applied when a key has no explicit limit.
type Limits struct {
	RequestsPerMin int64 `json:"requests_per_min"`
	TokensPerMin   int64 `json:"tokens_per_min"`
}

type snapshot struct {
	models []Model
	byName map[string]Model
	limits Limits
}

// Catalog is safe for concurrent use; reads are lock-free.
type Catalog struct{ v atomic.Pointer[snapshot] }

// New builds a catalog.
func New(models []Model, limits Limits) (*Catalog, error) {
	c := &Catalog{}
	if err := c.Replace(models, limits); err != nil {
		return nil, err
	}
	return c, nil
}

// FromConfig builds a catalog from configuration.
func FromConfig(cfg *config.Config) (*Catalog, error) {
	return New(ModelsFromConfig(cfg.Models), Limits{
		RequestsPerMin: cfg.RateLimit.DefaultRequestsPerMin,
		TokensPerMin:   cfg.RateLimit.DefaultTokensPerMin,
	})
}

// ModelsFromConfig converts configured models.
func ModelsFromConfig(ms []config.ModelConfig) []Model {
	out := make([]Model, 0, len(ms))
	for _, m := range ms {
		out = append(out, Model{Name: m.Name, Aliases: m.Aliases, DefaultMaxTokens: m.DefaultMaxTokens, ContextWindow: m.ContextWindow})
	}
	return out
}

// Replace swaps the whole catalog.
func (c *Catalog) Replace(models []Model, limits Limits) error {
	s := &snapshot{byName: map[string]Model{}, limits: limits}
	for _, m := range models {
		if m.Name == "" {
			return fmt.Errorf("catalog: model name is required")
		}
		for _, n := range append([]string{m.Name}, m.Aliases...) {
			if prev, dup := s.byName[n]; dup {
				return fmt.Errorf("catalog: %q is declared by both %q and %q", n, prev.Name, m.Name)
			}
			s.byName[n] = m
		}
		s.models = append(s.models, m)
	}
	sort.Slice(s.models, func(i, j int) bool { return s.models[i].Name < s.models[j].Name })
	c.v.Store(s)
	return nil
}

// SetModels replaces models, keeping limits.
func (c *Catalog) SetModels(models []Model) error { return c.Replace(models, c.Limits()) }

// SetLimits replaces default limits, keeping models.
func (c *Catalog) SetLimits(l Limits) {
	s := *c.v.Load()
	s.limits = l
	c.v.Store(&s)
}

// Resolve maps a client-facing name or alias to its catalog entry. Unknown
// names resolve to themselves (known=false) so models discovered from
// workers work without being declared.
func (c *Catalog) Resolve(name string) (m Model, known bool) {
	if m, ok := c.v.Load().byName[name]; ok {
		return m, true
	}
	return Model{Name: name}, false
}

// Models returns the declared models.
func (c *Catalog) Models() []Model { return append([]Model(nil), c.v.Load().models...) }

// Limits returns the default limits.
func (c *Catalog) Limits() Limits { return c.v.Load().limits }
