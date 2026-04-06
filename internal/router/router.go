// Package router picks the worker for a request. It is model-aware (only
// healthy workers advertising the model whose breaker admits traffic are
// candidates), prefers workers already holding the model in GPU memory, and
// balances load with power-of-two-choices.
package router

import (
	"errors"
	"math/rand/v2"
	"sync"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
)

// ErrNoWorkers is returned when no worker can serve the request.
var ErrNoWorkers = errors.New("no healthy worker available for model")

// Reason explains a routing decision; it is exported as a metric label and
// response header.
type Reason string

// Routing reasons.
const (
	ReasonSingle Reason = "single" // only one candidate
	ReasonP2C    Reason = "p2c"    // least-loaded of two random candidates
)

// coldPenalty is the load-equivalent cost of routing to a worker that must
// first load the model weights into GPU memory.
const coldPenalty = 8.0

// Config configures a Router.
type Config struct{}

// Request is the routing input.
type Request struct {
	Model string
	// Exclude lists worker IDs that already failed this request.
	Exclude map[string]bool
}

// Decision is the routing output.
type Decision struct {
	Worker     *registry.Worker
	Reason     Reason
	Candidates int
}

// Router selects workers from a registry.
type Router struct {
	reg *registry.Registry
	cfg Config

	mu  sync.Mutex
	rng *rand.Rand
}

// New returns a Router.
func New(reg *registry.Registry, cfg Config) *Router {
	return &Router{reg: reg, cfg: cfg, rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))}
}

// Config returns the router configuration.
func (r *Router) Config() Config { return r.cfg }

// Select picks a worker for req.
func (r *Router) Select(req Request) (Decision, error) {
	cands := r.candidates(req)
	switch len(cands) {
	case 0:
		return Decision{}, ErrNoWorkers
	case 1:
		return Decision{Worker: cands[0], Reason: ReasonSingle, Candidates: 1}, nil
	}
	w := r.p2c(req.Model, cands)
	return Decision{Worker: w, Reason: ReasonP2C, Candidates: len(cands)}, nil
}

func (r *Router) candidates(req Request) []*registry.Worker {
	all := r.reg.Candidates(req.Model)
	out := all[:0]
	for _, w := range all {
		if req.Exclude[w.ID] || !w.Breaker.Ready() {
			continue
		}
		out = append(out, w)
	}
	return out
}

// score is the weighted load including the cold-start penalty; lower is better.
func score(w *registry.Worker, model string) float64 {
	s := w.Load() + 1
	if !w.Warm(model) {
		s += coldPenalty
	}
	return s / w.Weight
}

// p2c samples two distinct candidates and returns the less loaded one. It
// avoids the herd behaviour of "always pick the global minimum" when load
// information is slightly stale, while staying exponentially better than
// random choice.
func (r *Router) p2c(model string, cands []*registry.Worker) *registry.Worker {
	r.mu.Lock()
	i := r.rng.IntN(len(cands))
	j := r.rng.IntN(len(cands) - 1)
	r.mu.Unlock()
	if j >= i {
		j++
	}
	a, b := cands[i], cands[j]
	sa, sb := score(a, model), score(b, model)
	if sb < sa || (sb == sa && b.TTFT.Quantiles(0.99)[0] < a.TTFT.Quantiles(0.99)[0]) {
		return b
	}
	return a
}
