package resilience

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"time"
)

// RetryPolicy configures exponential backoff with full jitter.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	// Rand returns a float in [0,1); defaults to math/rand.
	Rand func() float64
}

// Backoff returns the delay before attempt (1-based retry number). Full
// jitter — uniform in [0, min(max, base·2^n)] — de-synchronises clients that
// failed together so they do not stampede the surviving replicas.
func (p RetryPolicy) Backoff(retry int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	ceil := p.BaseDelay << min(retry, 20)
	if p.MaxDelay > 0 && (ceil > p.MaxDelay || ceil <= 0) {
		ceil = p.MaxDelay
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * float64(ceil))
}

// Retryable is implemented by errors that know whether a retry may help.
type Retryable interface{ Retryable() bool }

// IsRetryable reports whether err is worth retrying on another worker. The
// parent context's cancellation is never retryable.
func IsRetryable(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	var r Retryable
	if errors.As(err, &r) {
		return r.Retryable()
	}
	if errors.Is(err, ErrOpen) || errors.Is(err, ErrFirstTokenTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// ErrFirstTokenTimeout is the cancellation cause when a worker does not
// produce its first token in time.
var ErrFirstTokenTimeout = errors.New("worker did not produce a first token in time")

// Sleep waits for d or until ctx is done.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do runs fn until it succeeds, returns a non-retryable error, or attempts
// are exhausted.
func Do(ctx context.Context, p RetryPolicy, fn func(ctx context.Context, attempt int) error) error {
	attempts := max(p.MaxAttempts, 1)
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if serr := Sleep(ctx, p.Backoff(attempt)); serr != nil {
				return errors.Join(err, serr)
			}
		}
		if err = fn(ctx, attempt); err == nil || !IsRetryable(ctx, err) {
			return err
		}
	}
	return err
}
