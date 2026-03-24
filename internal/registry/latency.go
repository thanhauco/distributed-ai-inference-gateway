package registry

import (
	"slices"
	"sync"
	"time"
)

// LatencyWindow keeps the most recent samples in a ring buffer and answers
// quantile queries over them.
type LatencyWindow struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	full    bool
}

// NewLatencyWindow returns a window holding up to size samples.
func NewLatencyWindow(size int) *LatencyWindow {
	return &LatencyWindow{samples: make([]time.Duration, max(size, 1))}
}

// Observe records a sample.
func (w *LatencyWindow) Observe(d time.Duration) {
	w.mu.Lock()
	w.samples[w.next] = d
	w.next = (w.next + 1) % len(w.samples)
	if w.next == 0 {
		w.full = true
	}
	w.mu.Unlock()
}

// Quantiles returns the requested quantiles (0..1); zeros when empty.
func (w *LatencyWindow) Quantiles(qs ...float64) []time.Duration {
	w.mu.Lock()
	n := w.next
	if w.full {
		n = len(w.samples)
	}
	cp := slices.Clone(w.samples[:n])
	w.mu.Unlock()
	out := make([]time.Duration, len(qs))
	if n == 0 {
		return out
	}
	slices.Sort(cp)
	for i, q := range qs {
		idx := int(q*float64(n-1) + 0.5)
		out[i] = cp[min(max(idx, 0), n-1)]
	}
	return out
}
