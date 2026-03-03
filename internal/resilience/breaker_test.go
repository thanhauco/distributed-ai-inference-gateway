package resilience

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(cfg BreakerConfig) (*Breaker, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	b := NewBreaker(cfg)
	b.now = c.now
	return b, c
}

func TestBreakerTripsOnConsecutiveFailures(t *testing.T) {
	b, c := newTestBreaker(BreakerConfig{FailureThreshold: 3, OpenDuration: time.Second})
	var transitions []string
	b.OnStateChange(func(from, to State) { transitions = append(transitions, from.String()+">"+to.String()) })

	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess(0) // resets the streak
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != Closed {
		t.Fatal("tripped too early")
	}
	b.RecordFailure()
	if b.State() != Open || b.Allow() || b.Ready() {
		t.Fatal("breaker should be open")
	}

	c.add(time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("expected half-open, got %v", b.State())
	}
	if !b.Allow() {
		t.Fatal("half-open should admit one probe")
	}
	if b.Allow() || b.Ready() {
		t.Fatal("half-open should admit only one probe")
	}
	b.RecordFailure()
	if b.State() != Open {
		t.Fatal("failed probe should re-open")
	}
	c.add(time.Second)
	if !b.Allow() {
		t.Fatal("expected probe")
	}
	b.RecordSuccess(time.Millisecond)
	if b.State() != Closed {
		t.Fatal("successful probe should close")
	}
	want := []string{"closed>open", "open>half_open", "half_open>open", "open>half_open", "half_open>closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v", transitions)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions %v, want %v", transitions, want)
		}
	}
}

func TestBreakerTripsOnLatencyBreach(t *testing.T) {
	b, _ := newTestBreaker(BreakerConfig{FailureThreshold: 5, LatencyThreshold: 100 * time.Millisecond, SlowThreshold: 2})
	b.RecordSuccess(200 * time.Millisecond)
	b.RecordSuccess(10 * time.Millisecond) // fast call resets slow streak
	b.RecordSuccess(200 * time.Millisecond)
	if b.State() != Closed {
		t.Fatal("should still be closed")
	}
	b.RecordSuccess(300 * time.Millisecond)
	if b.State() != Open {
		t.Fatal("latency breach should trip")
	}
}

func TestBreakerReleaseFreesProbe(t *testing.T) {
	b, c := newTestBreaker(BreakerConfig{FailureThreshold: 1, OpenDuration: time.Second})
	b.RecordFailure()
	c.add(time.Second)
	if !b.Allow() {
		t.Fatal("probe expected")
	}
	b.Release()
	if !b.Allow() {
		t.Fatal("released probe slot should be reusable")
	}
}
