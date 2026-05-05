package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a fleet-wide sliding window limiter. Each key is two hashes
// (requests, tokens) of bucket-index → count, updated atomically by a Lua
// script so concurrent gateway replicas never over-admit. The {key} hash tag
// keeps both hashes in one Redis Cluster slot.
type Redis struct {
	client redis.UniversalClient
	cfg    Config
	prefix string
	now    func() time.Time
}

// NewRedis returns a Redis-backed limiter.
func NewRedis(client redis.UniversalClient, cfg Config) *Redis {
	return &Redis{client: client, cfg: cfg, prefix: "gw:rl:", now: time.Now}
}

// KEYS: req hash, tok hash
// ARGV: now_bucket, n_buckets, tokens, limit_req, limit_tok, ttl_ms
// Returns {allowed, reqs, toks, retry_bucket}; retry_bucket is the bucket
// whose expiry frees enough budget, or -1 when the request can never fit.
var reserveScript = redis.NewScript(`
local nowb = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local tokens = tonumber(ARGV[3])
local lr = tonumber(ARGV[4])
local lt = tonumber(ARGV[5])
local minb = nowb - n + 1

local function load(key)
  local total, per, stale = 0, {}, {}
  local kv = redis.call('HGETALL', key)
  for i = 1, #kv, 2 do
    local b, v = tonumber(kv[i]), tonumber(kv[i + 1])
    if b < minb then
      stale[#stale + 1] = kv[i]
    else
      total = total + v
      per[b] = (per[b] or 0) + v
    end
  end
  if #stale > 0 then redis.call('HDEL', key, unpack(stale)) end
  return total, per
end

local r, rper = load(KEYS[1])
local t, tper = load(KEYS[2])

local need, per = 0, nil
if lr > 0 and r + 1 > lr then
  need, per = r + 1 - lr, rper
elseif lt > 0 and t + tokens > lt then
  if tokens > lt then return {0, r, t, -1, 2} end
  need, per = t + tokens - lt, tper
end
if per then
  local freed = 0
  for b = minb, nowb do
    freed = freed + (per[b] or 0)
    if freed >= need then
      local reason = 1
      if per == tper then reason = 2 end
      return {0, r, t, b, reason}
    end
  end
  return {0, r, t, -1, 1}
end

redis.call('HINCRBY', KEYS[1], nowb, 1)
redis.call('HINCRBY', KEYS[2], nowb, tokens)
redis.call('PEXPIRE', KEYS[1], ARGV[6])
redis.call('PEXPIRE', KEYS[2], ARGV[6])
return {1, r + 1, t + tokens, 0, 0}
`)

func (r *Redis) keys(key string) []string {
	base := r.prefix + "{" + key + "}"
	return []string{base + ":r", base + ":t"}
}

// Reserve implements Limiter.
func (r *Redis) Reserve(ctx context.Context, key string, tokens int64, l Limits) (Decision, error) {
	now := r.now()
	bd := r.cfg.bucketDur()
	nowB := now.UnixNano() / int64(bd)
	ttl := (r.cfg.Window + bd).Milliseconds()
	res, err := reserveScript.Run(ctx, r.client, r.keys(key),
		nowB, r.cfg.buckets(), tokens, l.Requests, l.Tokens, ttl).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	allowed, reqs, toks, retryB, reason := res[0] == 1, res[1], res[2], res[3], res[4]
	d := Decision{
		Allowed: allowed, LimitRequests: l.Requests, LimitTokens: l.Tokens,
		RemainingRequests: remaining(l.Requests, reqs), RemainingTokens: remaining(l.Tokens, toks),
	}
	if !allowed {
		d.Reason = map[int64]string{1: "requests", 2: "tokens"}[reason]
		if retryB < 0 {
			d.RetryAfter = r.cfg.Window
		} else {
			d.RetryAfter = time.Unix(0, (retryB+r.cfg.buckets())*int64(bd)).Sub(now)
			if d.RetryAfter <= 0 {
				d.RetryAfter = bd
			}
		}
	}
	return d, nil
}

// Adjust implements Limiter.
func (r *Redis) Adjust(ctx context.Context, key string, delta int64) error {
	if delta == 0 {
		return nil
	}
	nowB := r.now().UnixNano() / int64(r.cfg.bucketDur())
	return r.client.HIncrBy(ctx, r.keys(key)[1], strconv.FormatInt(nowB, 10), delta).Err()
}
