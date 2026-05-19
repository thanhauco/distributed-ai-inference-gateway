package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// MemoryStore is a process-local Store.
type MemoryStore struct {
	mu         sync.Mutex
	maxEntries int
	exact      map[string]memItem
	buckets    map[string][]*Entry
	now        func() time.Time
}

type memItem struct {
	e      *Entry
	expiry time.Time
}

// NewMemoryStore returns a MemoryStore holding at most maxEntries exact
// entries (oldest-expiring evicted first).
func NewMemoryStore(maxEntries int) *MemoryStore {
	if maxEntries <= 0 {
		maxEntries = 10_000
	}
	return &MemoryStore{maxEntries: maxEntries, exact: map[string]memItem{}, buckets: map[string][]*Entry{}, now: time.Now}
}

// GetExact implements Store.
func (s *MemoryStore) GetExact(_ context.Context, key string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.exact[key]
	if !ok || s.now().After(it.expiry) {
		return nil, nil
	}
	return it.e, nil
}

// PutExact implements Store.
func (s *MemoryStore) PutExact(_ context.Context, key string, e *Entry, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.exact) >= s.maxEntries {
		s.evict()
	}
	s.exact[key] = memItem{e: e, expiry: s.now().Add(ttl)}
	return nil
}

func (s *MemoryStore) evict() {
	now := s.now()
	var oldestKey string
	var oldest time.Time
	for k, it := range s.exact {
		if now.After(it.expiry) {
			delete(s.exact, k)
			continue
		}
		if oldestKey == "" || it.expiry.Before(oldest) {
			oldestKey, oldest = k, it.expiry
		}
	}
	if len(s.exact) >= s.maxEntries && oldestKey != "" {
		delete(s.exact, oldestKey)
	}
}

// Candidates implements Store.
func (s *MemoryStore) Candidates(_ context.Context, buckets []string) ([]*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Entry
	for _, b := range buckets {
		out = append(out, s.buckets[b]...)
	}
	return out, nil
}

// AddCandidate implements Store.
func (s *MemoryStore) AddCandidate(_ context.Context, bucket string, e *Entry, _ time.Duration, maxEntries int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := append([]*Entry{e}, s.buckets[bucket]...)
	if len(list) > maxEntries {
		list = list[:maxEntries]
	}
	s.buckets[bucket] = list
	return nil
}

// RedisStore shares the cache across gateway replicas.
type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisStore returns a RedisStore.
func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client, prefix: "gw:cache:"}
}

// GetExact implements Store.
func (s *RedisStore) GetExact(ctx context.Context, key string) (*Entry, error) {
	raw, err := s.client.Get(ctx, s.prefix+"x:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// PutExact implements Store.
func (s *RedisStore) PutExact(ctx context.Context, key string, e *Entry, ttl time.Duration) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.prefix+"x:"+key, raw, ttl).Err()
}

// Candidates implements Store with one pipelined round trip.
func (s *RedisStore) Candidates(ctx context.Context, buckets []string) ([]*Entry, error) {
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(buckets))
	for i, b := range buckets {
		cmds[i] = pipe.LRange(ctx, s.prefix+"s:"+b, 0, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	var out []*Entry
	for _, cmd := range cmds {
		for _, raw := range cmd.Val() {
			var e Entry
			if json.Unmarshal([]byte(raw), &e) == nil {
				out = append(out, &e)
			}
		}
	}
	return out, nil
}

// AddCandidate implements Store.
func (s *RedisStore) AddCandidate(ctx context.Context, bucket string, e *Entry, ttl time.Duration, maxEntries int) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := s.prefix + "s:" + bucket
	pipe := s.client.TxPipeline()
	pipe.LPush(ctx, key, raw)
	pipe.LTrim(ctx, key, 0, int64(maxEntries-1))
	if ttl > 0 {
		pipe.PExpire(ctx, key, ttl)
	}
	_, err = pipe.Exec(ctx)
	return err
}
