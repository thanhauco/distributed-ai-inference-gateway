// Package resilience provides per-worker circuit breakers and retry policies
// that keep one slow or failing worker from dragging down fleet-wide p99.
package resilience

import (
	"errors"
	"sync"
	"time"
)

// State is a circuit breaker state.
type State int

// Breaker states.
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	}
	return "unknown"
}

// ErrOpen is returned when a breaker rejects a call.
var ErrOpen = errors.New("circuit breaker open")

// BreakerConfig configures a Breaker.
type BreakerConfig struct {
	// FailureThreshold trips the breaker after this many consecutive failures
	// (5xx, connection errors, first-token timeouts).
	FailureThreshold int
	// LatencyThreshold marks a successful call as slow when exceeded.
	LatencyThreshold time.Duration
	// SlowThreshold trips the breaker after this many consecutive slow calls.
	SlowThreshold int
	// OpenDuration is how long the breaker stays open before probing.
	OpenDuration time.Duration
	// HalfOpenProbes is the number of concurrent trial calls when half-open.
	HalfOpenProbes int
}

// Breaker is a consecutive-failure / latency-breach circuit breaker.
type Breaker struct {
	cfg BreakerConfig
	now func() time.Time

	mu              sync.Mutex
	state           State
	failures        int
	slow            int
	openedAt        time.Time
	halfOpenPending int
	onChange        func(from, to State)
}

// NewBreaker returns a closed breaker.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 15 * time.Second
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 1
	}
	return &Breaker{cfg: cfg, now: time.Now}
}

// OnStateChange registers a callback invoked (outside the lock) on transitions.
func (b *Breaker) OnStateChange(fn func(from, to State)) {
	b.mu.Lock()
	b.onChange = fn
	b.mu.Unlock()
}

// State returns the current state, promoting Open to HalfOpen once the open
// period has elapsed.
func (b *Breaker) State() State {
	b.mu.Lock()
	from, to := b.advance()
	s := b.state
	cb := b.onChange
	b.mu.Unlock()
	notify(cb, from, to)
	return s
}

// Ready reports, without reserving anything, whether a call would currently
// be admitted. Routers use it to filter candidates.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	from, to := b.advance()
	ok := b.state == Closed || (b.state == HalfOpen && b.halfOpenPending < b.cfg.HalfOpenProbes)
	cb := b.onChange
	b.mu.Unlock()
	notify(cb, from, to)
	return ok
}

// Allow admits a call, reserving a probe slot when half-open. Every admitted
// call must be followed by exactly one of RecordSuccess, RecordFailure or
// Release.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	from, to := b.advance()
	ok := false
	switch b.state {
	case Closed:
		ok = true
	case HalfOpen:
		if b.halfOpenPending < b.cfg.HalfOpenProbes {
			b.halfOpenPending++
			ok = true
		}
	}
	cb := b.onChange
	b.mu.Unlock()
	notify(cb, from, to)
	return ok
}

// RecordSuccess reports a successful call and its latency (time to first
// token for streaming calls).
func (b *Breaker) RecordSuccess(latency time.Duration) {
	b.mu.Lock()
	var from, to State = -1, -1
	b.failures = 0
	slow := b.cfg.LatencyThreshold > 0 && latency > b.cfg.LatencyThreshold
	switch b.state {
	case HalfOpen:
		b.releaseProbe()
		if slow {
			from, to = b.trip()
		} else {
			from, to = b.transition(Closed)
		}
	case Closed:
		if slow {
			b.slow++
			if b.cfg.SlowThreshold > 0 && b.slow >= b.cfg.SlowThreshold {
				from, to = b.trip()
			}
		} else {
			b.slow = 0
		}
	}
	cb := b.onChange
	b.mu.Unlock()
	notify(cb, from, to)
}

// RecordFailure reports a worker-attributable failure.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	var from, to State = -1, -1
	switch b.state {
	case HalfOpen:
		b.releaseProbe()
		from, to = b.trip()
	case Closed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			from, to = b.trip()
		}
	}
	cb := b.onChange
	b.mu.Unlock()
	notify(cb, from, to)
}

// Release returns a probe slot without recording an outcome, e.g. when the
// client went away before the worker answered.
func (b *Breaker) Release() {
	b.mu.Lock()
	if b.state == HalfOpen {
		b.releaseProbe()
	}
	b.mu.Unlock()
}

func (b *Breaker) releaseProbe() {
	if b.halfOpenPending > 0 {
		b.halfOpenPending--
	}
}

func (b *Breaker) trip() (State, State) {
	b.openedAt = b.now()
	return b.transition(Open)
}

// advance must be called with mu held.
func (b *Breaker) advance() (State, State) {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.OpenDuration {
		return b.transition(HalfOpen)
	}
	return -1, -1
}

// transition must be called with mu held.
func (b *Breaker) transition(to State) (State, State) {
	from := b.state
	if from == to {
		return -1, -1
	}
	b.state = to
	b.failures, b.slow, b.halfOpenPending = 0, 0, 0
	return from, to
}

func notify(cb func(from, to State), from, to State) {
	if cb != nil && from >= 0 && to >= 0 {
		cb(from, to)
	}
}
