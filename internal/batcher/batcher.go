// Package batcher groups requests for the same model that arrive within a
// few milliseconds of each other and hands them to the router together.
//
// The batch is not sent as one HTTP call — vLLM and Ollama already batch
// continuously on the GPU — but dispatching it as a unit lets the router place
// requests that share a prompt prefix on the same worker in the same
// scheduling step, where they share prefill and KV blocks.
//
// Batch size adapts to load: the target is the power of two nearest the
// number of requests expected within MaxWait at the observed arrival rate. At
// low traffic the target is 1 and requests pass straight through, so the
// batcher never adds latency when there is nothing to batch with.
package batcher

import (
	"context"
	"math"
	"math/bits"
	"sync"
	"time"
)

// Config configures a Batcher.
type Config struct {
	MinWait      time.Duration
	MaxWait      time.Duration
	MaxBatchSize int
}

// Batch is a group of jobs dispatched together.
type Batch[J any] struct {
	Key    string
	Jobs   []J
	Waited time.Duration // how long the oldest job waited
	Target int           // adaptive target size when the batch was opened
}

// Batcher accumulates jobs per key and dispatches them in batches.
type Batcher[J any] struct {
	cfg      Config
	dispatch func(Batch[J])
	now      func() time.Time

	mu     sync.Mutex
	queues map[string]*queue[J]
}

type pending[J any] struct {
	job  J
	done chan struct{}
}

type queue[J any] struct {
	items    []pending[J]
	openedAt time.Time
	target   int
	timer    *time.Timer
	rate     float64 // EWMA arrivals per second
	last     time.Time
}

// New returns a Batcher. dispatch is called synchronously from the
// goroutine that completes the batch; it must not block for long.
func New[J any](cfg Config, dispatch func(Batch[J])) *Batcher[J] {
	if cfg.MaxBatchSize < 1 {
		cfg.MaxBatchSize = 1
	}
	cfg.MaxBatchSize = floorPow2(cfg.MaxBatchSize)
	if cfg.MaxWait < cfg.MinWait {
		cfg.MaxWait = cfg.MinWait
	}
	return &Batcher[J]{cfg: cfg, dispatch: dispatch, now: time.Now, queues: map[string]*queue[J]{}}
}

// Submit enqueues job under key and blocks until its batch has been
// dispatched or ctx is done. A cancelled job is still dispatched with its
// batch; dispatchers should check the job's own context.
func (b *Batcher[J]) Submit(ctx context.Context, key string, job J) error {
	done := make(chan struct{})
	if batch, ok := b.enqueue(key, pending[J]{job: job, done: done}); ok {
		b.fire(batch) // this job completed the batch: dispatched inline
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type ready[J any] struct {
	batch Batch[J]
	items []pending[J]
}

func (b *Batcher[J]) enqueue(key string, p pending[J]) (ready[J], bool) {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[key]
	if q == nil {
		q = &queue[J]{}
		b.queues[key] = q
	}
	q.observe(now)
	q.items = append(q.items, p)
	if len(q.items) == 1 {
		q.openedAt = now
		q.target = b.targetSize(q.rate)
		if q.target > 1 {
			wait := b.waitFor(q.target, q.rate)
			q.timer = time.AfterFunc(wait, func() { b.flushTimer(key, q) })
		}
	}
	if len(q.items) >= q.target {
		return b.take(key, q, now), true
	}
	return ready[J]{}, false
}

func (b *Batcher[J]) flushTimer(key string, q *queue[J]) {
	b.mu.Lock()
	if b.queues[key] != q || len(q.items) == 0 {
		b.mu.Unlock()
		return
	}
	r := b.take(key, q, b.now())
	b.mu.Unlock()
	b.fire(r)
}

// take must be called with mu held.
func (b *Batcher[J]) take(key string, q *queue[J], now time.Time) ready[J] {
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	items := q.items
	q.items = nil
	jobs := make([]J, len(items))
	for i, it := range items {
		jobs[i] = it.job
	}
	return ready[J]{batch: Batch[J]{Key: key, Jobs: jobs, Waited: now.Sub(q.openedAt), Target: q.target}, items: items}
}

func (b *Batcher[J]) fire(r ready[J]) {
	b.dispatch(r.batch)
	for _, it := range r.items {
		close(it.done)
	}
}

// observe updates the arrival-rate EWMA.
func (q *queue[J]) observe(now time.Time) {
	if !q.last.IsZero() {
		dt := now.Sub(q.last).Seconds()
		inst := 1 / math.Max(dt, 1e-4)
		const alpha = 0.2
		if q.rate == 0 {
			q.rate = inst
		} else {
			q.rate = alpha*inst + (1-alpha)*q.rate
		}
	}
	q.last = now
}

// targetSize returns the power-of-two batch size expected to fill within
// MaxWait at the given arrival rate.
func (b *Batcher[J]) targetSize(rate float64) int {
	expected := rate * b.cfg.MaxWait.Seconds()
	if expected < 1.5 {
		return 1
	}
	return min(floorPow2(int(expected)), b.cfg.MaxBatchSize)
}

// waitFor bounds the wait to the time needed to fill target, clamped to
// [MinWait, MaxWait].
func (b *Batcher[J]) waitFor(target int, rate float64) time.Duration {
	fill := time.Duration(float64(target-1) / rate * float64(time.Second))
	return min(max(fill, b.cfg.MinWait), b.cfg.MaxWait)
}

// TargetSize exposes the current adaptive target for key (for metrics).
func (b *Batcher[J]) TargetSize(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if q := b.queues[key]; q != nil {
		return b.targetSize(q.rate)
	}
	return 1
}

func floorPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << (bits.Len(uint(n)) - 1)
}
