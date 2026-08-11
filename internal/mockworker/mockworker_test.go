package mockworker

import (
	"strings"
	"testing"
)

func TestPrefixCacheCountsLeadingBlocks(t *testing.T) {
	s := New(Config{PrefixCacheBlock: 4, PrefixCacheSize: 100})
	if hit := s.prefixLookup("aaaabbbbcccc"); hit != 0 {
		t.Fatalf("cold cache hit %d", hit)
	}
	if hit := s.prefixLookup("aaaabbbbddddeeee"); hit != 8 {
		t.Fatalf("shared prefix: hit %d, want 8", hit)
	}
	if hit := s.prefixLookup("xxxxaaaabbbb"); hit != 0 {
		t.Fatalf("prefix caching is positional: hit %d", hit)
	}
}

func TestPrefixCacheEvicts(t *testing.T) {
	s := New(Config{PrefixCacheBlock: 1, PrefixCacheSize: 3})
	s.prefixLookup("abcdef")
	if s.blocks.Len() != 3 {
		t.Fatalf("capacity not enforced: %d", s.blocks.Len())
	}
}

func TestCompletionDeterministic(t *testing.T) {
	a, fa := completion("seed", 100)
	b, fb := completion("seed", 100)
	if strings.Join(a, "") != strings.Join(b, "") || fa != fb {
		t.Fatal("completion must be deterministic")
	}
	c, fc := completion("seed", 3)
	if len(c) != 3 || fc != "length" {
		t.Fatalf("max tokens: %d %s", len(c), fc)
	}
}

func TestResidentModelsLRU(t *testing.T) {
	s := New(Config{Kind: "ollama", Models: []string{"phi3", "gemma2"}, MaxLoaded: 1})
	if !s.resident("phi3") {
		t.Fatal("first model is preloaded")
	}
	if s.resident("gemma2") {
		t.Fatal("gemma2 should be a cold load")
	}
	if s.resident("phi3") {
		t.Fatal("phi3 should have been evicted")
	}
}
