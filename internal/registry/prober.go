package registry

import (
	"context"
	"time"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/backend"
)

// Prober periodically refreshes every worker's health and load.
type Prober struct {
	Registry       *Registry
	Interval       time.Duration
	Timeout        time.Duration
	UnhealthyAfter int
	StaleAfter     time.Duration
	// OnProbe is called after every individual probe (for metrics).
	OnProbe func(*Worker, backend.ProbeResult, error)
}

// Run probes immediately and then every Interval until ctx is done.
func (p *Prober) Run(ctx context.Context) {
	p.tick(ctx)
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *Prober) tick(ctx context.Context) {
	p.Registry.EvictStale(p.StaleAfter)
	p.Registry.ProbeAll(ctx, p.Timeout, p.UnhealthyAfter, p.OnProbe)
}
