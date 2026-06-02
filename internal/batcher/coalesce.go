package batcher

import (
	"context"
	"sync"
)

// Group coalesces identical in-flight requests (singleflight). The first
// caller becomes the leader and generates; followers subscribe to the
// leader's stream and receive every chunk from the beginning, live. N
// identical prompts cost one generation instead of N.
type Group[T any] struct {
	mu      sync.Mutex
	flights map[string]*Flight[T]
}

// NewGroup returns an empty Group.
func NewGroup[T any]() *Group[T] {
	return &Group[T]{flights: map[string]*Flight[T]{}}
}

// Join returns the flight for key and whether the caller is its leader. The
// leader must call Finish exactly once.
func (g *Group[T]) Join(key string) (*Flight[T], bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if f, ok := g.flights[key]; ok {
		f.mu.Lock()
		f.followers++
		f.mu.Unlock()
		return f, false
	}
	f := &Flight[T]{changed: make(chan struct{}), group: g, key: key}
	g.flights[key] = f
	return f, true
}

// Flight is one in-progress generation that several callers share.
type Flight[T any] struct {
	group *Group[T]
	key   string

	mu        sync.Mutex
	items     []T
	done      bool
	err       error
	changed   chan struct{} // closed and replaced on every publish
	followers int
}

// Publish appends an item and wakes subscribers.
func (f *Flight[T]) Publish(item T) {
	f.mu.Lock()
	f.items = append(f.items, item)
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}

// Finish ends the flight. A non-nil err is delivered to followers, which
// should then fall back to generating on their own.
func (f *Flight[T]) Finish(err error) {
	f.group.mu.Lock()
	if f.group.flights[f.key] == f {
		delete(f.group.flights, f.key)
	}
	f.group.mu.Unlock()
	f.mu.Lock()
	f.done, f.err = true, err
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}

// Followers returns how many callers joined after the leader.
func (f *Flight[T]) Followers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.followers
}

// Subscribe calls fn for every item, from the first, until the flight
// finishes. It returns the flight's error, fn's error, or ctx's error.
func (f *Flight[T]) Subscribe(ctx context.Context, fn func(T) error) error {
	next := 0
	for {
		f.mu.Lock()
		batch := f.items[next:len(f.items):len(f.items)]
		done, err, changed := f.done, f.err, f.changed
		f.mu.Unlock()
		for _, it := range batch {
			if ferr := fn(it); ferr != nil {
				return ferr
			}
		}
		next += len(batch)
		if done {
			// Finish happens after the last Publish, so this snapshot held
			// every item.
			return err
		}
		if len(batch) > 0 {
			continue
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
