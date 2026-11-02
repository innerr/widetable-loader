package gen

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func pairConfig() Config {
	return Config{
		TotalRows:     10,
		Segments:      1,
		Scatter:       "hash32",
		Seed:          7,
		PairCatalog:   6000,
		PairFill:      100,
		PairRawDist:   []PairRaw{{96, 0.7}, {192, 0.25}, {512, 0.05}},
		PairTimeStart: 1792022400000,
		PairTimeSpan:  259200000,
		PairIndex: PairIndex{
			DWDistinct: 100, RowsPerPgID: 250, RowsPerOssPath: 4,
			RowsPerAnonymousID: 1000, RowsPerAttachment: 4,
			RowsPerPackageName: 100, RowsPerParentID: 1000,
		},
	}
}

func TestPairRowShape(t *testing.T) {
	g, err := NewGenerator(pairConfig(), 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	cols, vals, err := g.NextPairs(0)
	if err != nil {
		t.Fatalf("next pairs: %v", err)
	}
	want := 1 + len(pairSystemColumns) + len(pairIndexColumns) + 2*100
	if len(cols) != want || len(vals) != want {
		t.Fatalf("want %d columns and values, got %d and %d", want, len(cols), len(vals))
	}
	if cols[0] != "id" {
		t.Fatalf("first column is %q, want id", cols[0])
	}
	seen := map[string]bool{}
	rawLengths := map[int]int{}
	for i, name := range cols {
		if seen[name] {
			t.Fatalf("duplicate column %q", name)
		}
		seen[name] = true
		if !strings.HasPrefix(name, "xx_") {
			continue
		}
		var pair map[string]any
		val := strings.Trim(vals[i], "'")
		if err := json.Unmarshal([]byte(val), &pair); err != nil {
			t.Fatalf("raw column %s is not valid JSON: %v", name, err)
		}
		rawLengths[len(val)]++
		if len(val) != 96 && len(val) != 192 && len(val) != 512 {
			t.Fatalf("raw column %s has length %d, want 96/192/512", name, len(val))
		}
		valName := cols[i-1]
		if !strings.HasSuffix(valName, "_val") || len(valName) < 4 ||
			strings.TrimPrefix(name, "xx_") != strings.Replace(valName[3:], "_val", "_raw", 1) {
			t.Fatalf("raw column %s does not pair with %s", name, valName)
		}
	}
	if rawLengths[96] == 0 || rawLengths[192] == 0 || rawLengths[512] == 0 {
		t.Fatalf("raw length distribution missing a bucket: %v", rawLengths)
	}
}

func TestPairRowsAreReproducible(t *testing.T) {
	first, err := NewGenerator(pairConfig(), 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	second, err := NewGenerator(pairConfig(), 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	for seq := uint64(0); seq < 3; seq++ {
		colsA, valsA, err := first.NextPairs(seq)
		if err != nil {
			t.Fatalf("next pairs: %v", err)
		}
		colsB, valsB, err := second.NextPairs(seq)
		if err != nil {
			t.Fatalf("next pairs: %v", err)
		}
		if strings.Join(colsA, ",") != strings.Join(colsB, ",") ||
			strings.Join(valsA, ",") != strings.Join(valsB, ",") {
			t.Fatalf("row %d differs between generators with the same seed", seq)
		}
	}
}

func TestPairFieldSelectionIsDistinct(t *testing.T) {
	cfg := pairConfig()
	cfg.TotalRows = 20
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	for seq := uint64(0); seq < 20; seq++ {
		cols, _, err := g.NextPairs(seq)
		if err != nil {
			t.Fatalf("next pairs: %v", err)
		}
		families := map[string]bool{}
		ids := map[string]bool{}
		for _, name := range cols[len(cols)-200:] {
			if !strings.HasSuffix(name, "_val") {
				continue
			}
			families[name[:2]] = true
			if ids[name] {
				t.Fatalf("row %d repeats field %s", seq, name)
			}
			ids[name] = true
		}
		if len(families) < 3 {
			t.Fatalf("row %d only covers families %v", seq, families)
		}
	}
}

func TestPairSystemColumnsFollowIndexCardinality(t *testing.T) {
	cfg := pairConfig()
	cfg.TotalRows = 1000
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	values := func(seq uint64) map[string]string {
		cols, vals, err := g.NextPairs(seq)
		if err != nil {
			t.Fatalf("next pairs: %v", err)
		}
		out := map[string]string{}
		for i, name := range cols {
			out[name] = vals[i]
		}
		return out
	}
	first := values(0)
	if got := first["dwId"]; got != "0" {
		t.Fatalf("dwId at 0 is %q, want 0", got)
	}
	if got := values(100)["dwId"]; got != "0" {
		t.Fatalf("dwId at 100 is %q, want wrap to 0", got)
	}
	if got := values(4)["ms_ossPath"]; got != "'oss/1'" {
		t.Fatalf("ossPath at 4 is %q, want 'oss/1'", got)
	}
	if got := values(1)["ms_dataUniqueId"]; got != "'du-1'" {
		t.Fatalf("dataUniqueId at 1 is %q, want 'du-1'", got)
	}
	if got := values(0)["mi_isDeleted"]; got != "1" {
		t.Fatalf("mi_isDeleted at 0 is %q, want 1", got)
	}
	if got := values(50)["mi_isDeleted"]; got != "2" {
		t.Fatalf("mi_isDeleted at 50 is %q, want 2", got)
	}
	if got := values(7)["mi_isDeleted"]; got != "0" {
		t.Fatalf("mi_isDeleted at 7 is %q, want 0", got)
	}
}

func TestParsePairRawDist(t *testing.T) {
	got, err := ParsePairRawDist("96:0.7, 192:0.25,512:0.05")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []PairRaw{{96, 0.7}, {192, 0.25}, {512, 0.05}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, err := ParsePairRawDist("96"); err == nil {
		t.Fatalf("expected an error for a spec without weight")
	}
}

func TestPairValidationRejectsBadCatalog(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"catalog not divisible by five": func(c *Config) { c.PairCatalog = 6001 },
		"fill above catalog":            func(c *Config) { c.PairFill = 7000 },
		"missing raw distribution":      func(c *Config) { c.PairRawDist = nil },
		"zero time span":                func(c *Config) { c.PairTimeSpan = 0 },
	} {
		cfg := pairConfig()
		mutate(&cfg)
		if _, err := NewGenerator(cfg, 0); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}
