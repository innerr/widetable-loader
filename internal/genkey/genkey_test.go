package genkey

import (
	"slices"
	"testing"
)

// Engine-verified hash32 values (SELECT hash32(...) on the test cluster).
func TestEngineHash32(t *testing.T) {
	for _, c := range []struct {
		id   uint64
		want uint32
	}{
		{0, 3530670207},
		{1, 2484513939},
		{2, 19522071},
		{100, 3465648511},
	} {
		if got := EngineHash32(c.id); got != c.want {
			t.Fatalf("EngineHash32(%d) = %d, want %d", c.id, got, c.want)
		}
	}
}

func TestKeySeq(t *testing.T) {
	const total = 1000
	layout := Hash32Prefix{}
	ks := BuildKeySeq(total, layout.ID)

	// Engine-key order is strictly ascending on (hash, id) and each rank's
	// entry matches its seq.
	prevH, prevID := uint32(0), uint64(0)
	for r := uint64(0); r < total; r++ {
		h, id := ks.HashAt(r), ks.IDAt(r)
		if r > 0 && (h < prevH || (h == prevH && id <= prevID)) {
			t.Fatalf("rank %d (%d,%d) not ascending after (%d,%d)", r, h, id, prevH, prevID)
		}
		if id != layout.ID(ks.SeqAt(r)) {
			t.Fatalf("rank %d: IDAt %d != ID(SeqAt %d)", r, id, ks.SeqAt(r))
		}
		prevH, prevID = h, id
	}

	// SeqAt covers the whole sequence domain exactly once.
	seqs := append([]uint64(nil), ksWithSeqs(ks)...)
	slices.Sort(seqs)
	for i, s := range seqs {
		if s != uint64(i) {
			t.Fatalf("SeqAt not a permutation: sorted[%d]=%d", i, s)
		}
	}

	// Consecutive segments share no key range.
	const rowsPerSeg = 250
	for s := uint64(0); s+1 < total/rowsPerSeg; s++ {
		_, maxA := ks.SegmentBounds(s, rowsPerSeg)
		minB, _ := ks.SegmentBounds(s+1, rowsPerSeg)
		if ks.HashAt(s*rowsPerSeg+rowsPerSeg-1) >= ks.HashAt((s+1)*rowsPerSeg) &&
			maxA >= minB {
			t.Fatalf("segment %d and %d ranges not disjoint", s, s+1)
		}
	}
}

func ksWithSeqs(ks *KeySeq) []uint64 { return ks.seqs }
