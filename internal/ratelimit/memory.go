package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Memory is a single-process sliding window limiter. Use Redis when the
// gateway runs more than one replica and limits must be global.
type Memory struct {
	cfg Config
	now func() time.Time

	mu   sync.Mutex
	keys map[string]*window
	ops  int
}

type window struct {
	idx  []int64 // bucket index stored in each slot
	reqs []int64
	toks []int64
}

// NewMemory returns an in-memory limiter.
func NewMemory(cfg Config) *Memory {
	return &Memory{cfg: cfg, now: time.Now, keys: map[string]*window{}}
}

func (m *Memory) bucket(now time.Time) int64 {
	return now.UnixNano() / int64(m.cfg.bucketDur())
}

func (m *Memory) get(key string) *window {
	w, ok := m.keys[key]
	if !ok {
		n := m.cfg.buckets()
		w = &window{idx: make([]int64, n), reqs: make([]int64, n), toks: make([]int64, n)}
		for i := range w.idx {
			w.idx[i] = -1
		}
		m.keys[key] = w
	}
	return w
}

func (w *window) at(b int64, field []int64) int64 {
	slot := b % int64(len(w.idx))
	if w.idx[slot] != b {
		return 0
	}
	return field[slot]
}

func (w *window) sum(nowB int64) (reqs, toks int64) {
	n := int64(len(w.idx))
	for i := range w.idx {
		if b := w.idx[i]; b > nowB-n && b <= nowB {
			reqs += w.reqs[i]
			toks += w.toks[i]
		}
	}
	return
}

func (w *window) add(b, reqs, toks int64) {
	slot := b % int64(len(w.idx))
	if w.idx[slot] != b {
		w.idx[slot], w.reqs[slot], w.toks[slot] = b, 0, 0
	}
	w.reqs[slot] += reqs
	w.toks[slot] += toks
}

// Reserve implements Limiter.
func (m *Memory) Reserve(_ context.Context, key string, tokens int64, l Limits) (Decision, error) {
	now := m.now()
	nowB := m.bucket(now)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gc(nowB)
	w := m.get(key)
	reqs, toks := w.sum(nowB)
	d := Decision{LimitRequests: l.Requests, LimitTokens: l.Tokens}
	switch {
	case l.Requests > 0 && reqs+1 > l.Requests:
		d.Reason = "requests"
		d.RetryAfter = retryAfter(m.cfg, now, nowB, reqs+1-l.Requests, func(b int64) int64 { return w.at(b, w.reqs) })
	case l.Tokens > 0 && toks+tokens > l.Tokens:
		d.Reason = "tokens"
		need := toks + tokens - l.Tokens
		if tokens > l.Tokens {
			need = toks + tokens + 1 // unsatisfiable
		}
		d.RetryAfter = retryAfter(m.cfg, now, nowB, need, func(b int64) int64 { return w.at(b, w.toks) })
	default:
		w.add(nowB, 1, tokens)
		reqs, toks = reqs+1, toks+tokens
		d.Allowed = true
	}
	d.RemainingRequests = remaining(l.Requests, reqs)
	d.RemainingTokens = remaining(l.Tokens, toks)
	return d, nil
}

// Adjust implements Limiter.
func (m *Memory) Adjust(_ context.Context, key string, delta int64) error {
	if delta == 0 {
		return nil
	}
	nowB := m.bucket(m.now())
	m.mu.Lock()
	m.get(key).add(nowB, 0, delta)
	m.mu.Unlock()
	return nil
}

// gc drops idle keys every few thousand operations. Must hold mu.
func (m *Memory) gc(nowB int64) {
	m.ops++
	if m.ops%4096 != 0 {
		return
	}
	n := m.cfg.buckets()
	for k, w := range m.keys {
		idle := true
		for _, b := range w.idx {
			if b > nowB-n {
				idle = false
				break
			}
		}
		if idle {
			delete(m.keys, k)
		}
	}
}
