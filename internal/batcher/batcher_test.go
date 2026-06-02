package batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLowTrafficPassesThrough(t *testing.T) {
	var batches []Batch[int]
	b := New(Config{MinWait: 5 * time.Millisecond, MaxWait: 20 * time.Millisecond, MaxBatchSize: 16},
		func(bt Batch[int]) { batches = append(batches, bt) })
	start := time.Now()
	for i := range 3 {
		if err := b.Submit(context.Background(), "m", i); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // 20 req/s × 20ms ≈ 0.4 expected → target 1
	}
	if len(batches) != 3 {
		t.Fatalf("expected 3 singleton batches, got %d", len(batches))
	}
	for _, bt := range batches {
		if len(bt.Jobs) != 1 || bt.Waited > time.Millisecond {
			t.Fatalf("singleton batch waited %v", bt.Waited)
		}
	}
	if time.Since(start) > 400*time.Millisecond {
		t.Fatal("pass-through added latency")
	}
}

func TestHighTrafficFormsPowerOfTwoBatches(t *testing.T) {
	var mu sync.Mutex
	sizes := map[int]int{}
	var maxWait time.Duration
	b := New(Config{MinWait: 5 * time.Millisecond, MaxWait: 20 * time.Millisecond, MaxBatchSize: 12},
		func(bt Batch[int]) {
			mu.Lock()
			sizes[len(bt.Jobs)]++
			if bt.Waited > maxWait {
				maxWait = bt.Waited
			}
			if bt.Target&(bt.Target-1) != 0 || bt.Target > 8 {
				t.Errorf("target %d is not a power of two ≤ 8", bt.Target)
			}
			mu.Unlock()
		})
	var wg sync.WaitGroup
	for i := range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Submit(context.Background(), "m", i)
		}()
		time.Sleep(250 * time.Microsecond) // ~4000 req/s
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if sizes[8] == 0 {
		t.Fatalf("expected full batches of 8 (max 12 floors to 8), got %v", sizes)
	}
	if maxWait > 40*time.Millisecond {
		t.Fatalf("batch waited %v, beyond max wait", maxWait)
	}
}

func TestKeysBatchIndependently(t *testing.T) {
	var got sync.Map
	b := New(Config{MaxWait: time.Millisecond, MaxBatchSize: 4}, func(bt Batch[string]) {
		for _, j := range bt.Jobs {
			got.Store(j, bt.Key)
		}
	})
	_ = b.Submit(context.Background(), "a", "x")
	_ = b.Submit(context.Background(), "b", "y")
	if k, _ := got.Load("x"); k != "a" {
		t.Fatal("wrong key")
	}
	if k, _ := got.Load("y"); k != "b" {
		t.Fatal("wrong key")
	}
}

func TestInlineDispatchSucceedsDespiteSlowDispatcher(t *testing.T) {
	b := New(Config{MaxWait: time.Millisecond, MaxBatchSize: 1}, func(Batch[int]) { time.Sleep(30 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	// MaxBatchSize 1 dispatches inline: once dispatch ran, the job was routed.
	if err := b.Submit(ctx, "k", 1); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedSubmitHonoursContext(t *testing.T) {
	b := New(Config{MinWait: time.Second, MaxWait: time.Second, MaxBatchSize: 16}, func(Batch[int]) {})
	q := &queue[int]{rate: 1000} // pretend traffic is high so the batch waits
	b.queues["k"] = q
	q.last = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := b.Submit(ctx, "k", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Submit ignored its context")
	}
}

func TestFloorPow2(t *testing.T) {
	for in, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 2, 8: 8, 15: 8, 16: 16, 17: 16} {
		if got := floorPow2(in); got != want {
			t.Errorf("floorPow2(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestCoalesceFollowersSeeEverything(t *testing.T) {
	g := NewGroup[string]()
	f, leader := g.Join("k")
	if !leader {
		t.Fatal("first caller leads")
	}
	f.Publish("a") // published before the follower joins

	var wg sync.WaitGroup
	results := make([]string, 3)
	for i := range 3 {
		ff, lead := g.Join("k")
		if lead || ff != f {
			t.Fatal("followers must join the existing flight")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ff.Subscribe(context.Background(), func(s string) error { results[i] += s; return nil })
		}()
	}
	f.Publish("b")
	f.Publish("c")
	f.Finish(nil)
	wg.Wait()
	for _, r := range results {
		if r != "abc" {
			t.Fatalf("follower saw %q", r)
		}
	}
	if f.Followers() != 3 {
		t.Fatalf("followers = %d", f.Followers())
	}
	if _, lead := g.Join("k"); !lead {
		t.Fatal("finished flight must be removed")
	}
}

func TestCoalesceError(t *testing.T) {
	g := NewGroup[int]()
	f, _ := g.Join("k")
	ff, _ := g.Join("k")
	boom := errors.New("boom")
	go f.Finish(boom)
	if err := ff.Subscribe(context.Background(), func(int) error { return nil }); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestCoalesceSubscriberCancel(t *testing.T) {
	g := NewGroup[int]()
	f, _ := g.Join("k")
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if err := f.Subscribe(ctx, func(int) error { n.Add(1); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
