// Package genkey maps a per-row sequence number to a primary key under a
// chosen scatter layout. All layouts are bijective over the sequence domain,
// so uniqueness and segment disjointness hold by construction.
package genkey

import (
	"encoding/binary"
	"math/bits"
	"slices"
	"strconv"
	"sync"
)

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

// Hash32Prefix combines a 31-bit hash prefix with the low 32 sequence bits.
// Keys fit signed BIGINT and are distinct for sequences below 2^32.
type Hash32Prefix struct{}

func (l Hash32Prefix) ID(seq uint64) uint64 {
	x := seq
	// Mix the sequence, then retain 31 hash bits in the positive key.
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x = x ^ (x >> 31)
	return ((x << 32) | (seq & 0xffffffff)) & positiveMask
}

// KeySeq orders every sequence by its engine key. The wide-table primary key
// is (hash32(id), id): the engine renders id as its decimal ASCII string,
// hashes it with MurmurHash3 x86_32 (seed 0), and packs the uint32 hash
// big-endian in front of the sign-flipped big-endian id, so ascending
// (hash, id) numeric order equals the engine key byte order (verified
// against SELECT hash32(...) and rdb_datadic.cc pack_long/pack_longlong).
// Segments cut from this order share no engine-key range by construction.
type KeySeq struct {
	seqs []uint64 // seq holding the i-th smallest engine key
	ids  []uint64 // id of the i-th smallest engine key (parallel to seqs)
}

// EngineHash32 replicates the engine's SQL hash32() for non-negative ids.
func EngineHash32(id uint64) uint32 {
	return murmur3_32([]byte(strconv.FormatUint(id, 10)))
}

// murmur3_32 is the standard MurmurHash3 x86_32, seed 0 (mysys/my_murmur3.cc).
func murmur3_32(data []byte) uint32 {
	const c1, c2 uint32 = 0xcc9e2d51, 0x1b873593
	h1 := uint32(0)
	for i := 0; i+4 <= len(data); i += 4 {
		k1 := binary.LittleEndian.Uint32(data[i:])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}
	tail := data[len(data)&^3:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}
	h1 ^= uint32(len(data))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}

// BuildKeySeq orders every sequence by engine key; idOf maps a sequence to
// its id (the scatter layout). Cost is one full sort (~24 bytes per row).
func BuildKeySeq(total uint64, idOf func(uint64) uint64) *KeySeq {
	type ent struct {
		h   uint32
		id  uint64
		seq uint64
	}
	ents := make([]ent, total)
	for s := uint64(0); s < total; s++ {
		id := idOf(s)
		ents[s] = ent{h: EngineHash32(id), id: id, seq: s}
	}
	slices.SortFunc(ents, func(a, b ent) int {
		if a.h != b.h {
			if a.h < b.h {
				return -1
			}
			return 1
		}
		switch {
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	})
	ks := &KeySeq{seqs: make([]uint64, total), ids: make([]uint64, total)}
	for i, e := range ents {
		ks.seqs[i] = e.seq
		ks.ids[i] = e.id
	}
	return ks
}

// SeqAt returns the sequence holding the given rank in engine key order.
func (k *KeySeq) SeqAt(rank uint64) uint64 { return k.seqs[rank] }

// IDAt returns the id holding the given rank (smallest engine key first).
func (k *KeySeq) IDAt(rank uint64) uint64 { return k.ids[rank] }

// HashAt returns the engine hash32 of the id at the given rank.
func (k *KeySeq) HashAt(rank uint64) uint32 { return EngineHash32(k.ids[rank]) }

// Total returns the ordered sequence count.
func (k *KeySeq) Total() uint64 { return uint64(len(k.seqs)) }

// SegmentBounds returns the [min, max] id range of a consecutive-rank
// segment; adjacent segments share no engine keys by construction.
func (k *KeySeq) SegmentBounds(segment, rowsPerSeg uint64) (uint64, uint64) {
	lo := segment * rowsPerSeg
	return k.ids[lo], k.ids[lo+rowsPerSeg-1]
}

// keySeqCache memoizes one KeySeq per process: a daemon generating many
// segments of the same dataset sorts once instead of once per segment.
// A 1e8-row table costs about 2.4 GB, held until process exit.
var keySeqCache struct {
	sync.Mutex
	total uint64
	ks    *KeySeq
}

// SharedKeySeq returns the cached ordering for total, building it on first
// use. Rows keep the Hash32Prefix id formula; the order is the engine key
// (hash32(id), id), so only the keyseq scatter needs the table.
func SharedKeySeq(total uint64) *KeySeq {
	keySeqCache.Lock()
	defer keySeqCache.Unlock()
	if keySeqCache.ks == nil || keySeqCache.total != total {
		keySeqCache.ks = BuildKeySeq(total, Hash32Prefix{}.ID)
		keySeqCache.total = total
	}
	return keySeqCache.ks
}
