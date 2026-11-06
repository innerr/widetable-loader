package check

import (
	"strings"
	"testing"

	"github.com/innerr/widetable-loader/internal/gen"
)

func TestValueComparison(t *testing.T) {
	cfg := gen.Config{}
	for _, row := range []struct {
		idx       int
		want, got string
		equal     bool
	}{
		{1, "7", "7", true}, {1, "7", "8", false},
		{3, "1.500000", "1.5", true}, {3, "1.5", "1.6", false},
		{4, "true", "false", false},
		{5, `{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{5, `{"a":1}`, `{"a":2}`, false},
	} {
		if equalValue(row.idx, cfg, row.want, row.got) != row.equal {
			t.Fatal(row)
		}
	}
}

func TestPairValueComparison(t *testing.T) {
	extraRaw := `{"v":"value","p":"` + strings.Repeat("a", 51200) + `"}`
	cases := []struct {
		want, got string
		ok        bool
	}{
		{"'oss/12'", "oss/12", true},
		{"'oss/12'", "oss/13", false},
		{"12345", "12345", true},
		{"12345", "12346", false},
		{"12.34", "12.34", true},
		{"12.34", "12.340", true},
		{"true", "1", true},
		{"true", "0", false},
		{"false", "0", true},
		{"false", "1", false},
		{"'" + extraRaw + "'", extraRaw, true},
	}
	for _, c := range cases {
		if got := pairValueEqual(c.want, c.got); got != c.ok {
			t.Fatalf("pairValueEqual(%q, %q) = %v, want %v", c.want, c.got, got, c.ok)
		}
	}
}

func TestPairJSONColumnComparison(t *testing.T) {
	jsonCols := map[string]bool{"mj_long_1": true}
	cases := []struct {
		name      string
		column    string
		want, got string
		equal     bool
	}{
		{name: "JSON key order", column: "mj_long_1", want: `'{"a":1,"b":2}'`, got: `{"b":2,"a":1}`, equal: true},
		{name: "JSON whitespace", column: "mj_long_1", want: `'{"a":1,"b":2}'`, got: `{ "a": 1, "b": 2 }`, equal: true},
		{name: "JSON number format", column: "mj_long_1", want: `'{"n":1.0}'`, got: `{"n":1}`, equal: true},
		{name: "JSON different value", column: "mj_long_1", want: `'{"a":1}'`, got: `{"a":2}`, equal: false},
		{name: "text key order", column: "xx_long_1_raw", want: `'{"a":1,"b":2}'`, got: `{"b":2,"a":1}`, equal: false},
		{name: "text whitespace", column: "xx_long_1_raw", want: `'{"a":1,"b":2}'`, got: `{"a":1, "b":2}`, equal: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pairColumnValueEqual(tc.column, tc.want, tc.got, jsonCols); got != tc.equal {
				t.Fatalf("pairColumnValueEqual(%q, %q, %q) = %v, want %v", tc.column, tc.want, tc.got, got, tc.equal)
			}
		})
	}
}

func TestSampleOffsetsLimitedToHead(t *testing.T) {
	offs := sampleOffsets(1000, 1, 3, 8, 20)
	if len(offs) != 8 {
		t.Fatalf("got %d offsets, want 8", len(offs))
	}
	for _, off := range offs {
		if off >= 20 {
			t.Fatalf("offset %d is outside the 20-row head", off)
		}
	}
}

func TestSegmentLimit(t *testing.T) {
	for _, tc := range []struct {
		segments    uint64
		maxSegments int
		want        uint64
	}{
		{segments: 100, maxSegments: 0, want: 100},
		{segments: 100, maxSegments: 5, want: 5},
		{segments: 3, maxSegments: 5, want: 3},
	} {
		if got := segmentLimit(tc.segments, tc.maxSegments); got != tc.want {
			t.Fatalf("segmentLimit(%d, %d) = %d, want %d", tc.segments, tc.maxSegments, got, tc.want)
		}
	}
}
