package gen

import (
	"bufio"
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPairExtraRaw(t *testing.T) {
	cfg := pairConfig()
	cfg.TotalRows = 200
	cfg.FillBuckets = 1
	cfg.PairExtraRaw = []PairExtraRaw{
		{Name: "xx_long_1_raw", Bytes: 51200},
		{Name: "xx_long_2_raw", Bytes: 51200},
	}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	cols, vals, err := g.NextPairs(0)
	if err != nil {
		t.Fatalf("next pairs: %v", err)
	}
	extraAt := 1 + len(pairSystemColumns) + len(pairIndexColumns)
	firstPairAt := extraAt + len(cfg.PairExtraRaw)
	if !strings.HasSuffix(cols[firstPairAt], "_val") {
		t.Fatalf("column after extra raw values is %q, want a pair value column", cols[firstPairAt])
	}
	first := make([]string, len(cfg.PairExtraRaw))
	for i, extra := range cfg.PairExtraRaw {
		at := extraAt + i
		if cols[at] != extra.Name {
			t.Fatalf("column %d is %q, want %q", at, cols[at], extra.Name)
		}
		if len(vals[at]) < 2 || vals[at][0] != '\'' || vals[at][len(vals[at])-1] != '\'' {
			t.Fatalf("extra raw value %q is not a quoted SQL literal", extra.Name)
		}
		raw := vals[at][1 : len(vals[at])-1]
		if len(raw) != extra.Bytes {
			t.Fatalf("extra raw value %q has %d bytes, want %d", extra.Name, len(raw), extra.Bytes)
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			t.Fatalf("extra raw value %q is not valid JSON: %v", extra.Name, err)
		}
		first[i] = raw
	}
	wantCols := append([]string(nil), cols...)
	secondCols, secondVals, err := g.NextPairs(1)
	if err != nil {
		t.Fatalf("next second pair row: %v", err)
	}
	if !reflect.DeepEqual(secondCols, wantCols) {
		t.Fatal("extra raw columns changed between rows")
	}
	for i, extra := range cfg.PairExtraRaw {
		second := secondVals[extraAt+i][1 : len(secondVals[extraAt+i])-1]
		if second == first[i] {
			t.Fatalf("extra raw value %q did not change between rows", extra.Name)
		}
	}
	frame := pairColumnFrame(cfg.PairCatalog, cfg.PairExtraRaw)
	for i, extra := range cfg.PairExtraRaw {
		if frame[extraAt+i] != extra.Name {
			t.Fatalf("frame column %d is %q, want %q", extraAt+i, frame[extraAt+i], extra.Name)
		}
	}
	if !strings.HasSuffix(frame[firstPairAt], "_val") {
		t.Fatalf("frame column after extra raw values is %q, want a pair value column", frame[firstPairAt])
	}

	writeGen, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatalf("new write generator: %v", err)
	}
	var out bytes.Buffer
	if err := writeGen.WriteSQLPairsMultiRow(bufio.NewWriter(&out), "loader_perf.import_rows", 50); err != nil {
		t.Fatalf("write multi-row pairs: %v", err)
	}
	if got := bytes.Count(out.Bytes(), []byte("INSERT INTO ")); got != 4 {
		t.Fatalf("got %d INSERT statements, want 4", got)
	}
}

func TestParsePairExtraRaw(t *testing.T) {
	got, err := ParsePairExtraRaw("xx_long_1_raw:51200, xx_long_2_raw: 51200")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []PairExtraRaw{{Name: "xx_long_1_raw", Bytes: 51200}, {Name: "xx_long_2_raw", Bytes: 51200}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	encoded, err := json.Marshal(got[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != `{"name":"xx_long_1_raw","bytes":51200}` {
		t.Fatalf("unexpected JSON encoding: %s", encoded)
	}
	for _, spec := range []string{"xx_long_1_raw", "xx_long_1_raw:large"} {
		if _, err := ParsePairExtraRaw(spec); err == nil {
			t.Fatalf("expected an error for %q", spec)
		}
	}
}

func TestPairExtraRawValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		extra []PairExtraRaw
		max   int
	}{
		{name: "zero bytes", extra: []PairExtraRaw{{Name: "xx_long_raw", Bytes: 0}}},
		{name: "negative bytes", extra: []PairExtraRaw{{Name: "xx_long_raw", Bytes: -1}}},
		{name: "too small", extra: []PairExtraRaw{{Name: "xx_long_raw", Bytes: 63}}},
		{name: "empty name", extra: []PairExtraRaw{{Name: " ", Bytes: 51200}}},
		{name: "single quote", extra: []PairExtraRaw{{Name: "xx_long'_raw", Bytes: 51200}}},
		{name: "backslash", extra: []PairExtraRaw{{Name: `xx_long\_raw`, Bytes: 51200}}},
		{name: "comma", extra: []PairExtraRaw{{Name: "xx_long,_raw", Bytes: 51200}}},
		{name: "backtick", extra: []PairExtraRaw{{Name: "xx_long`_raw", Bytes: 51200}}},
		{name: "system column", extra: []PairExtraRaw{{Name: "dataType", Bytes: 51200}}},
		{name: "index column", extra: []PairExtraRaw{{Name: "ms_ossPath", Bytes: 51200}}},
		{name: "dynamic raw column", extra: []PairExtraRaw{{Name: "xx_000001_raw", Bytes: 51200}}},
		{name: "duplicate", extra: []PairExtraRaw{{Name: "xx_long_raw", Bytes: 51200}, {Name: "xx_long_raw", Bytes: 51200}}},
		{name: "extra raw above custom limit", extra: []PairExtraRaw{{Name: "xx_long_1_raw", Bytes: 51200}, {Name: "xx_long_2_raw", Bytes: 51200}}, max: 102400 - 1},
		{name: "pair row and extra raw above custom limit", extra: []PairExtraRaw{{Name: "xx_long_1_raw", Bytes: 51200}, {Name: "xx_long_2_raw", Bytes: 51200}}, max: 156000 - 1},
		{name: "negative limit", max: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := pairConfig()
			cfg.PairExtraRaw = test.extra
			cfg.PairMaxRowBytes = test.max
			if _, err := NewGenerator(cfg, 0); err == nil {
				t.Fatal("expected validation to fail")
			}
		})
	}
}
