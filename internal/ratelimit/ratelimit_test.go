package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type limiterUnderTest struct {
	name    string
	l       Limiter
	advance func(time.Duration)
}

func limiters(t *testing.T) []limiterUnderTest {
	cfg := Config{Window: time.Minute, Buckets: 60}
	base := time.Unix(1_700_000_000, 0)

	memNow := base
	mem := NewMemory(cfg)
	mem.now = func() time.Time { return memNow }

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	redisNow := base
	rl := NewRedis(client, cfg)
	rl.now = func() time.Time { return redisNow }

	return []limiterUnderTest{
		{"memory", mem, func(d time.Duration) { memNow = memNow.Add(d) }},
		{"redis", rl, func(d time.Duration) { redisNow = redisNow.Add(d); mr.FastForward(d) }},
	}
}

func TestRequestLimit(t *testing.T) {
	for _, tc := range limiters(t) {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lim := Limits{Requests: 3}
			for i := range 3 {
				d, err := tc.l.Reserve(ctx, "k", 1, lim)
				if err != nil || !d.Allowed {
					t.Fatalf("req %d denied: %+v %v", i, d, err)
				}
				if d.RemainingRequests != int64(2-i) {
					t.Fatalf("remaining %d", d.RemainingRequests)
				}
			}
			d, _ := tc.l.Reserve(ctx, "k", 1, lim)
			if d.Allowed || d.Reason != "requests" {
				t.Fatalf("4th request should be denied: %+v", d)
			}
			if d.RetryAfter <= 0 || d.RetryAfter > time.Minute {
				t.Fatalf("retry after %v", d.RetryAfter)
			}
			if d2, _ := tc.l.Reserve(ctx, "other", 1, lim); !d2.Allowed {
				t.Fatal("keys must be isolated")
			}
		})
	}
}

func TestTokenLimitSlides(t *testing.T) {
	for _, tc := range limiters(t) {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lim := Limits{Tokens: 1000}
			if d, _ := tc.l.Reserve(ctx, "k", 600, lim); !d.Allowed {
				t.Fatal("first denied")
			}
			tc.advance(30 * time.Second)
			if d, _ := tc.l.Reserve(ctx, "k", 300, lim); !d.Allowed || d.RemainingTokens != 100 {
				t.Fatalf("second: %+v", d)
			}
			d, _ := tc.l.Reserve(ctx, "k", 200, lim)
			if d.Allowed || d.Reason != "tokens" {
				t.Fatalf("third should be denied: %+v", d)
			}
			// The 600-token bucket expires 60s after it was written: 30s from now.
			if d.RetryAfter < 29*time.Second || d.RetryAfter > 31*time.Second {
				t.Fatalf("retry after %v, want ~30s", d.RetryAfter)
			}
			tc.advance(31 * time.Second)
			if d, _ := tc.l.Reserve(ctx, "k", 200, lim); !d.Allowed {
				t.Fatalf("window should have slid: %+v", d)
			}
		})
	}
}

func TestOversizedRequest(t *testing.T) {
	for _, tc := range limiters(t) {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := tc.l.Reserve(context.Background(), "k", 5000, Limits{Tokens: 1000})
			if d.Allowed || d.RetryAfter != time.Minute {
				t.Fatalf("oversized: %+v", d)
			}
		})
	}
}

func TestAdjustReconcilesUsage(t *testing.T) {
	for _, tc := range limiters(t) {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lim := Limits{Tokens: 1000}
			// Estimated 900 (prompt + max_tokens) but only 100 were used.
			_, _ = tc.l.Reserve(ctx, "k", 900, lim)
			if err := tc.l.Adjust(ctx, "k", -800); err != nil {
				t.Fatal(err)
			}
			if d, _ := tc.l.Reserve(ctx, "k", 800, lim); !d.Allowed {
				t.Fatalf("refund not applied: %+v", d)
			}
		})
	}
}

func TestUnlimited(t *testing.T) {
	for _, tc := range limiters(t) {
		d, _ := tc.l.Reserve(context.Background(), "k", 1<<40, Limits{})
		if !d.Allowed || d.RemainingTokens != -1 {
			t.Fatalf("%s: zero limits mean unlimited: %+v", tc.name, d)
		}
	}
}
