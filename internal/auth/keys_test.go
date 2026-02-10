package auth

import (
	"errors"
	"strings"
	"testing"
)

func TestCreateAuthenticateRevoke(t *testing.T) {
	s := NewStore()
	raw, k, err := s.Create(Spec{Name: "team-a", TokensPerMin: 1000, Models: []string{"llama3-8b"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, KeyPrefix) || len(raw) != len(KeyPrefix)+40 {
		t.Fatalf("unexpected key format %q", raw)
	}
	got, err := s.Authenticate(raw)
	if err != nil || got.ID != k.ID {
		t.Fatalf("authenticate: %v %v", got, err)
	}
	if !got.AllowsModel("llama3-8b") || got.AllowsModel("phi3") {
		t.Fatal("model allow-list not enforced")
	}
	if err := s.Revoke(k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(raw); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("revoked key still valid: %v", err)
	}
	if err := s.Revoke(k.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestMissingKey(t *testing.T) {
	if _, err := NewStore().Authenticate(""); !errors.Is(err, ErrMissingKey) {
		t.Fatal(err)
	}
}

func TestReplaceSourceKeepsAdminKeys(t *testing.T) {
	s := NewStore()
	s.Add("cfg-1", Spec{Name: "a"}, "config")
	adminRaw, _, _ := s.Create(Spec{Name: "b"})
	s.ReplaceSource("config", map[string]Spec{"cfg-2": {Name: "c"}})
	if _, err := s.Authenticate("cfg-1"); err == nil {
		t.Fatal("old config key should be gone")
	}
	for _, raw := range []string{"cfg-2", adminRaw} {
		if _, err := s.Authenticate(raw); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	if len(s.List()) != 2 {
		t.Fatalf("list = %d", len(s.List()))
	}
}

func TestBearerToken(t *testing.T) {
	for in, want := range map[string]string{"Bearer abc": "abc", "bearer  xyz ": "xyz", "Basic abc": "", "": ""} {
		if got := BearerToken(in); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", in, got, want)
		}
	}
}
