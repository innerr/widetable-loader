package gen

import (
	"testing"
)

func nullPattern(rec []string) []bool {
	out := make([]bool, len(rec))
	for i, v := range rec {
		out[i] = v == ""
	}
	return out
}

func equalBool(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPairsCSVAndBuckets(t *testing.T) {
	cfg := Config{TotalRows: 200, Segments: 1, Scatter: "none", Seed: 7,
		PairCatalog: 20, PairFill: 4, FillBuckets: 4,
		PairRawDist:   []PairRaw{{Bytes: 64, Weight: 1}},
		PairTimeStart: 1, PairTimeSpan: 100,
		PairIndex: PairIndex{DWDistinct: 10, RowsPerPgID: 2, RowsPerOssPath: 3, RowsPerAnonymousID: 4, RowsPerAttachment: 5, RowsPerPackageName: 6, RowsPerParentID: 7}}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	names := PairColumnNames(cfg)
	if len(names) != 1+7+6+2*20 {
		t.Fatalf("frame size %d", len(names))
	}
	var first []string
	for seq := uint64(0); seq < 200; seq++ {
		rec, err := g.NextPairsRecord(seq)
		if err != nil {
			t.Fatal(err)
		}
		if len(rec) != len(names) {
			t.Fatalf("record size %d != %d", len(rec), len(names))
		}
		if !equalBool(nullPattern(rec), nullPattern(mustRecord(t, g, seq%4))) {
			t.Fatalf("bucket cycle broken at row %d", seq)
		}
		if seq == 0 {
			first = rec
		}
	}
	filled := 0
	for _, v := range first {
		if v != "" {
			filled++
		}
	}
	if filled != 1+7+6+2*4 {
		t.Fatalf("filled columns %d", filled)
	}
}

func mustRecord(t *testing.T, g *Generator, seq uint64) []string {
	rec, err := g.NextPairsRecord(seq)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestPairsBucketRows(t *testing.T) {
	cfg := Config{TotalRows: 200, Segments: 1, Scatter: "none", Seed: 7,
		PairCatalog: 20, PairFill: 4, FillBuckets: 4, PairBucketRows: 50,
		PairRawDist:   []PairRaw{{Bytes: 64, Weight: 1}},
		PairTimeStart: 1, PairTimeSpan: 100,
		PairIndex: PairIndex{DWDistinct: 10, RowsPerPgID: 2, RowsPerOssPath: 3, RowsPerAnonymousID: 4, RowsPerAttachment: 5, RowsPerPackageName: 6, RowsPerParentID: 7}}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	prev := mustRecord(t, g, 0)
	for seq := uint64(1); seq < 200; seq++ {
		rec := mustRecord(t, g, seq)
		if seq%50 == 0 {
			if equalBool(nullPattern(rec), nullPattern(prev)) {
				t.Fatalf("row %d should start a new bucket", seq)
			}
		} else if !equalBool(nullPattern(rec), nullPattern(prev)) {
			t.Fatalf("row %d broke its 50-row bucket", seq)
		}
		prev = rec
	}
}
