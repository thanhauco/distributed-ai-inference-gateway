// Package ratelimit implements token-level sliding window rate limiting.
//
// Each key has two budgets per window: requests and tokens. Counting tokens
// (prompt estimate + max completion, reconciled with actual usage after the
// response) rather than requests stops a client from staying under a request
// quota while sending a few enormous prompts. The window is split into
// buckets so the limit slides smoothly instead of resetting at a boundary,
// which would allow a 2× burst across the edge.
package ratelimit

import (
	"context"
	"time"
)

// Limits are the per-window budgets for one key. Zero means unlimited.
type Limits struct {
	Requests int64
	Tokens   int64
}

// Decision is the outcome of a reservation.
type Decision struct {
	Allowed           bool
	Reason            string // "requests" or "tokens" when denied
	RetryAfter        time.Duration
	LimitRequests     int64
	LimitTokens       int64
	RemainingRequests int64
	RemainingTokens   int64
}

// Limiter is implemented by the in-memory and Redis backends.
type Limiter interface {
	// Reserve admits one request costing tokens, or denies it.
	Reserve(ctx context.Context, key string, tokens int64, limits Limits) (Decision, error)
	// Adjust corrects the token count of the current bucket once actual
	// usage is known. delta may be negative.
	Adjust(ctx context.Context, key string, delta int64) error
}

// Config configures window geometry.
type Config struct {
	Window  time.Duration
	Buckets int
}

func (c Config) bucketDur() time.Duration {
	if c.Buckets <= 0 {
		c.Buckets = 60
	}
	if c.Window <= 0 {
		c.Window = time.Minute
	}
	return c.Window / time.Duration(c.Buckets)
}

func (c Config) buckets() int64 {
	if c.Buckets <= 0 {
		return 60
	}
	return int64(c.Buckets)
}

// retryAfter computes how long until enough of the oldest buckets expire to
// free need units. per maps bucket index to usage, oldest first from minB.
func retryAfter(cfg Config, now time.Time, nowB, need int64, per func(b int64) int64) time.Duration {
	n := cfg.buckets()
	bd := cfg.bucketDur()
	var freed int64
	for b := nowB - n + 1; b <= nowB; b++ {
		freed += per(b)
		if freed >= need {
			expiry := time.Unix(0, (b+n)*int64(bd))
			if d := expiry.Sub(now); d > 0 {
				return d
			}
			return bd
		}
	}
	// The request alone exceeds the budget: it can never be admitted, but
	// signal a full window rather than "retry immediately".
	return cfg.Window
}

func remaining(limit, used int64) int64 {
	if limit <= 0 {
		return -1
	}
	return max(limit-used, 0)
}
