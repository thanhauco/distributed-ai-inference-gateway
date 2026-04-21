package router

import (
	"container/list"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sort"
	"sync"

	"github.com/thanhauco/distributed-ai-inference-gateway/internal/registry"
	"github.com/thanhauco/distributed-ai-inference-gateway/pkg/openai"
)

// Keys derives the routing keys from a conversation.
//
//   - conv covers every message up to and including the first user message:
//     all later turns of the same conversation share it, so they can stick to
//     the worker that already holds the conversation's KV blocks.
//   - group covers the leading system messages: unrelated conversations that
//     share a long system prompt hash to the same worker, so its prefix cache
//     serves the prompt once instead of once per worker.
//
// Both are bounded by maxChars, mirroring how far a prefix cache can help.
func Keys(msgs []openai.Message, maxChars int) (conv, group uint64) {
	if maxChars <= 0 {
		maxChars = 4096
	}
	hc := fnv.New64a()
	hg := fnv.New64a()
	budget := maxChars
	inSystem, hasSystem := true, false
	for _, m := range msgs {
		if budget <= 0 {
			break
		}
		content := m.Content
		if len(content) > budget {
			content = content[:budget]
		}
		budget -= len(content)
		write := func(h interface{ Write([]byte) (int, error) }) {
			_, _ = h.Write([]byte(m.Role))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte(content))
			_, _ = h.Write([]byte{0})
		}
		write(hc)
		if inSystem && m.Role == "system" {
			write(hg)
			hasSystem = true
		} else {
			inSystem = false
		}
		if m.Role == "user" {
			break
		}
	}
	conv = nonZero(hc.Sum64())
	group = conv // no system prompt: the conversation itself is the best group
	if hasSystem {
		group = nonZero(hg.Sum64())
	}
	return conv, group
}

// SessionKey derives a conversation key from an explicit session identifier.
func SessionKey(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("session\x00" + id))
	return nonZero(h.Sum64())
}

func nonZero(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	return v
}

// selectAffinity implements sticky + bounded-load rendezvous hashing
// ("consistent hashing with bounded loads"): a worker is eligible only while
// its load stays under LoadFactor × the weighted average, which caps hot-spot
// skew while keeping placement stable as workers come and go.
func (r *Router) selectAffinity(req Request, cands []*registry.Worker) (Decision, bool) {
	var totalLoad, totalWeight float64
	for _, w := range cands {
		totalLoad += w.Load()
		totalWeight += w.Weight
	}
	over := func(w *registry.Worker) bool {
		capacity := r.cfg.LoadFactor * (totalLoad + 1) * w.Weight / totalWeight
		return w.Load()+1 > math.Ceil(capacity)
	}

	if req.ConvKey != 0 {
		if id, ok := r.table.get(req.ConvKey); ok {
			for _, w := range cands {
				if w.ID == id && !over(w) {
					return Decision{Worker: w, Reason: ReasonAffinitySticky, Candidates: len(cands)}, true
				}
			}
		}
	}

	key := req.GroupKey
	if key == 0 {
		key = req.ConvKey
	}
	ranked := rank(key, cands)
	for i, w := range ranked {
		if over(w) {
			continue
		}
		reason := ReasonAffinityHash
		if i > 0 {
			reason = ReasonAffinitySpill
		}
		r.remember(req, w)
		return Decision{Worker: w, Reason: reason, Candidates: len(cands)}, true
	}
	return Decision{}, false
}

// rank orders workers by weighted rendezvous (HRW) score for key. Removing a
// worker only moves the keys it owned; adding one only steals ~1/n of keys.
func rank(key uint64, cands []*registry.Worker) []*registry.Worker {
	type scored struct {
		w *registry.Worker
		s float64
	}
	ss := make([]scored, len(cands))
	for i, w := range cands {
		ss[i] = scored{w, hrw(key, w.ID, w.Weight)}
	}
	sort.Slice(ss, func(i, j int) bool {
		if ss[i].s != ss[j].s {
			return ss[i].s > ss[j].s
		}
		return ss[i].w.ID < ss[j].w.ID
	})
	out := make([]*registry.Worker, len(ss))
	for i := range ss {
		out[i] = ss[i].w
	}
	return out
}

// hrw is the weighted rendezvous score -w/ln(u), u uniform in (0,1).
func hrw(key uint64, id string, weight float64) float64 {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], key)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(id))
	x := mix(h.Sum64())
	u := (float64(x>>11) + 0.5) / float64(1<<53)
	return -weight / math.Log(u)
}

// mix is the splitmix64 finalizer; FNV alone has weak avalanche on the low bits.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// lruTable remembers which worker served each conversation.
type lruTable struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[uint64]*list.Element
}

type lruEntry struct {
	key uint64
	val string
}

func newLRUTable(capacity int) *lruTable {
	return &lruTable{cap: capacity, ll: list.New(), items: map[uint64]*list.Element{}}
}

func (t *lruTable) get(k uint64) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.items[k]; ok {
		t.ll.MoveToFront(e)
		return e.Value.(*lruEntry).val, true
	}
	return "", false
}

func (t *lruTable) put(k uint64, v string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.items[k]; ok {
		e.Value.(*lruEntry).val = v
		t.ll.MoveToFront(e)
		return
	}
	t.items[k] = t.ll.PushFront(&lruEntry{k, v})
	if t.ll.Len() > t.cap {
		last := t.ll.Back()
		t.ll.Remove(last)
		delete(t.items, last.Value.(*lruEntry).key)
	}
}

func (t *lruTable) dropValue(v string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for e := t.ll.Front(); e != nil; {
		next := e.Next()
		if ent := e.Value.(*lruEntry); ent.val == v {
			t.ll.Remove(e)
			delete(t.items, ent.key)
		}
		e = next
	}
}

func (t *lruTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ll.Len()
}
