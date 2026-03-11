package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffFullJitter(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, Rand: func() float64 { return 0.999999 }}
	if d := p.Backoff(1); d < 199*time.Millisecond || d > 200*time.Millisecond {
		t.Fatalf("retry 1 ceiling: %v", d)
	}
	if d := p.Backoff(10); d > time.Second {
		t.Fatalf("not capped: %v", d)
	}
	p.Rand = func() float64 { return 0 }
	if p.Backoff(3) != 0 {
		t.Fatal("full jitter lower bound should be 0")
	}
}

type retryErr bool

func (r retryErr) Error() string   { return "x" }
func (r retryErr) Retryable() bool { return bool(r) }

func TestDo(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Microsecond}
	calls := 0
	err := Do(context.Background(), p, func(ctx context.Context, attempt int) error {
		calls++
		if attempt < 2 {
			return retryErr(true)
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	calls = 0
	err = Do(context.Background(), p, func(context.Context, int) error { calls++; return retryErr(false) })
	if err == nil || calls != 1 {
		t.Fatalf("non-retryable should stop: calls=%d", calls)
	}
}

func TestIsRetryable(t *testing.T) {
	ctx := context.Background()
	if !IsRetryable(ctx, ErrFirstTokenTimeout) || !IsRetryable(ctx, ErrOpen) {
		t.Fatal("expected retryable")
	}
	if IsRetryable(ctx, errors.New("boom")) {
		t.Fatal("plain error should not be retryable")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if IsRetryable(cancelled, ErrOpen) {
		t.Fatal("cancelled parent must not retry")
	}
}
