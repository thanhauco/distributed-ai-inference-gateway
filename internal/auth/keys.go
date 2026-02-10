// Package auth implements API key authentication. Keys are stored only as
// SHA-256 digests; the raw secret is returned once, at creation time.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// KeyPrefix marks gateway-issued keys.
const KeyPrefix = "sk-gw-"

var (
	// ErrMissingKey is returned when no credential was presented.
	ErrMissingKey = errors.New("missing API key")
	// ErrInvalidKey is returned for unknown or revoked keys.
	ErrInvalidKey = errors.New("invalid API key")
	// ErrNotFound is returned when a key ID does not exist.
	ErrNotFound = errors.New("api key not found")
)

// Key is an API key's metadata and limits.
type Key struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Hint           string    `json:"hint"` // first characters of the secret, for humans
	RequestsPerMin int64     `json:"requests_per_min"`
	TokensPerMin   int64     `json:"tokens_per_min"`
	Models         []string  `json:"models,omitempty"` // empty means all models
	CreatedAt      time.Time `json:"created_at"`
	Source         string    `json:"source"` // config | admin
	hash           [32]byte
}

// AllowsModel reports whether the key may call model.
func (k *Key) AllowsModel(model string) bool {
	return len(k.Models) == 0 || slices.Contains(k.Models, model)
}

// Store is a concurrency-safe API key store.
type Store struct {
	mu     sync.RWMutex
	byHash map[[32]byte]*Key
	byID   map[string]*Key
	now    func() time.Time
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{byHash: map[[32]byte]*Key{}, byID: map[string]*Key{}, now: time.Now}
}

// Spec describes a key to add.
type Spec struct {
	Name           string
	RequestsPerMin int64
	TokensPerMin   int64
	Models         []string
}

// Add registers an existing raw key (e.g. from configuration).
func (s *Store) Add(raw string, spec Spec, source string) *Key {
	h := sha256.Sum256([]byte(raw))
	k := &Key{
		ID:             "key_" + hex.EncodeToString(h[:6]),
		Name:           spec.Name,
		Hint:           hint(raw),
		RequestsPerMin: spec.RequestsPerMin,
		TokensPerMin:   spec.TokensPerMin,
		Models:         append([]string(nil), spec.Models...),
		CreatedAt:      s.now().UTC(),
		Source:         source,
		hash:           h,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.byHash[h]; ok {
		delete(s.byID, old.ID)
	}
	s.byHash[h] = k
	s.byID[k.ID] = k
	return k
}

// Create mints a new random key and returns the raw secret exactly once.
func (s *Store) Create(spec Spec) (string, *Key, error) {
	raw, err := generate(40)
	if err != nil {
		return "", nil, err
	}
	raw = KeyPrefix + raw
	return raw, s.Add(raw, spec, "admin"), nil
}

// Revoke removes a key by ID.
func (s *Store) Revoke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	delete(s.byID, id)
	delete(s.byHash, k.hash)
	return nil
}

// Authenticate resolves a raw key to its metadata.
func (s *Store) Authenticate(raw string) (*Key, error) {
	if raw == "" {
		return nil, ErrMissingKey
	}
	h := sha256.Sum256([]byte(raw))
	s.mu.RLock()
	k, ok := s.byHash[h]
	s.mu.RUnlock()
	// The map lookup is keyed by digest, so timing does not leak the secret;
	// the constant-time compare guards against a future map-less lookup.
	if !ok || subtle.ConstantTimeCompare(k.hash[:], h[:]) != 1 {
		return nil, ErrInvalidKey
	}
	return k, nil
}

// List returns all keys sorted by name.
func (s *Store) List() []*Key {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Key, 0, len(s.byID))
	for _, k := range s.byID {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name || (out[i].Name == out[j].Name && out[i].ID < out[j].ID)
	})
	return out
}

// ReplaceSource atomically swaps every key from source with keys, leaving
// keys from other sources untouched. Used on config reload.
func (s *Store) ReplaceSource(source string, keys map[string]Spec) {
	s.mu.Lock()
	for id, k := range s.byID {
		if k.Source == source {
			delete(s.byID, id)
			delete(s.byHash, k.hash)
		}
	}
	s.mu.Unlock()
	for raw, spec := range keys {
		s.Add(raw, spec, source)
	}
}

// BearerToken extracts the token from an Authorization header value.
func BearerToken(header string) string {
	const p = "bearer "
	if len(header) > len(p) && strings.EqualFold(header[:len(p)], p) {
		return strings.TrimSpace(header[len(p):])
	}
	return ""
}

func hint(raw string) string {
	if len(raw) <= 10 {
		return "****"
	}
	return raw[:10] + "…"
}

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func generate(n int) (string, error) {
	b := make([]byte, n)
	max := big.NewInt(int64(len(alphabet)))
	for i := range b {
		v, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		b[i] = alphabet[v.Int64()]
	}
	return string(b), nil
}
