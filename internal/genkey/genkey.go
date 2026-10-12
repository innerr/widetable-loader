// Package genkey maps a per-row sequence number to a primary key under a
// chosen scatter layout. All layouts are bijective over the sequence domain,
// so uniqueness and segment disjointness hold by construction.
package genkey

// Knuth multiplicative constant (golden ratio, odd).
const mulA = uint64(0x9E3779B97F4A7C15)

// positiveMask keeps generated keys inside the signed BIGINT positive range.
const positiveMask = uint64(1<<63 - 1)

// Layout maps a global row sequence number to a primary key value.
type Layout interface {
	ID(globalSeq uint64) uint64
}

// Identity: key = base + seq. Segment bases are spaced by the caller:
// tight spacing gives a fully contiguous key space ("none"), loose spacing
// leaves gaps between segments for post-import writes ("blocked").
type Identity struct{ Base uint64 }

func (l Identity) ID(seq uint64) uint64 { return l.Base + seq }

// MulHash: key = (seq*A + C) mod 2^63. A is odd, so this is a bijection;
// keys are spread evenly across the whole key domain with no long runs of
// zero low bits (the failure mode of bit-reversal for non-power-of-two N).
type MulHash struct{ C uint64 }

func (l MulHash) ID(seq uint64) uint64 { return (seq*mulA + l.C) & positiveMask }

// NewMulHash derives C from the seed so different seeds give different
// permutations.
func NewMulHash(seed uint64) MulHash {
	return MulHash{C: (seed*mulA + 0x517cc1b727220a95) & positiveMask}
}
