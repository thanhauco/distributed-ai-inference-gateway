package cache

import (
	"math/rand/v2"
)

// LSH buckets unit vectors with random-hyperplane (SimHash) signatures:
// vectors with high cosine similarity share most signature bits, so a lookup
// only has to scan a handful of buckets instead of every cached entry.
type LSH struct {
	planes [][]float32
}

// NewLSH builds bits hyperplanes for vectors of dimension dims. The seed is
// fixed so every gateway replica computes identical signatures.
func NewLSH(bits, dims int) *LSH {
	rng := rand.New(rand.NewPCG(0x5eed, uint64(dims)))
	planes := make([][]float32, bits)
	for i := range planes {
		p := make([]float32, dims)
		for j := range p {
			p[j] = float32(rng.NormFloat64())
		}
		planes[i] = p
	}
	return &LSH{planes: planes}
}

// Signature returns the bucket signature of v.
func (l *LSH) Signature(v []float32) uint32 {
	var sig uint32
	for i, p := range l.planes {
		var dot float32
		for j := range p {
			if j < len(v) {
				dot += p[j] * v[j]
			}
		}
		if dot >= 0 {
			sig |= 1 << i
		}
	}
	return sig
}

// Probes returns sig plus every signature at Hamming distance 1. Multi-probe
// recovers near-duplicates that landed on the other side of one hyperplane.
func (l *LSH) Probes(sig uint32) []uint32 {
	out := make([]uint32, 0, len(l.planes)+1)
	out = append(out, sig)
	for i := range l.planes {
		out = append(out, sig^(1<<i))
	}
	return out
}
