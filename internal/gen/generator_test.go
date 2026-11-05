package gen

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/innerr/widetable-loader/internal/genkey"
)

func TestGenerationDoesNotOverwriteFiles(t *testing.T) {
	out := filepath.Join(t.TempDir(), "data", "seg.csv")
	args := []string{"-total-rows", "2", "-segments", "1", "-fields", "2", "-fixed-cols", "0",
		"-index-fields", "0", "-fill", "1", "-out", out}
	if err := Run(args); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(out)
	if err := Run(args); err == nil {
		t.Fatal("existing output overwritten")
	}
	after, _ := os.ReadFile(out)
	if !bytes.Equal(before, after) {
		t.Fatal("existing data changed")
	}
}

func TestLayoutsAndDeterminism(t *testing.T) {
	for _, scatter := range []string{"blocked", "none", "mul-hash", "hash32"} {
		t.Run(scatter, func(t *testing.T) {
			cfg := Config{TotalRows: 24, Segments: 3, Scatter: scatter, GapFactor: 2,
				Fields: 5, Fill: 3, IndexFields: 1, RowWidth: 32, Seed: 7}
			keys := map[string]bool{}
			for seg := uint64(0); seg < cfg.Segments; seg++ {
				g, err := NewGenerator(cfg, seg)
				if err != nil {
					t.Fatal(err)
				}
				for seq := seg * 8; seq < (seg+1)*8; seq++ {
					rec, err := g.Next(seq)
					if err != nil {
						t.Fatal(err)
					}
					if keys[rec[0]] {
						t.Fatalf("duplicate key %s", rec[0])
					}
					keys[rec[0]] = true
					if scatter == "none" && g.layout.ID(seq) != seq {
						t.Fatal("none layout has gaps")
					}
					if g.layout.ID(seq) >= 1<<63 {
						t.Fatal("signed BIGINT overflow")
					}
				}
				var a, b bytes.Buffer
				for _, dst := range []*bytes.Buffer{&a, &b} {
					g, _ := NewGenerator(cfg, seg)
					w := bufio.NewWriter(dst)
					if err := g.WriteCSV(w); err != nil {
						t.Fatal(err)
					}
					if err := w.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(a.Bytes(), b.Bytes()) {
					t.Fatal("generation not deterministic")
				}
			}
		})
	}
}

func TestSQLFormatFullFill(t *testing.T) {
	// fill == file-cols: every row has the same filled set, so statements
	// are exactly multiRow rows (the last one holds the remainder).
	cfg := Config{TotalRows: 10, Segments: 1, Scatter: "none", Fields: 5, FixedCols: 1,
		IndexFields: 1, Fill: 5, RowWidth: 32, Seed: 7}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := bufio.NewWriter(&out)
	if err := g.WriteSQL(w, "loader_perf.import_rows", 3, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("want 4 statements (3+3+3+1), got %d", len(lines))
	}
	total := 0
	for i, line := range lines {
		rows := strings.Count(line, "),(") + 1
		want := 3
		if i == 3 {
			want = 1
		}
		if rows != want {
			t.Fatalf("statement %d has %d rows, want %d", i, rows, want)
		}
		total += rows
		if !strings.HasPrefix(line, "INSERT INTO `loader_perf`.`import_rows` (`id`") {
			t.Fatalf("bad statement prefix: %.60s", line)
		}
		if !strings.HasSuffix(line, ");") {
			t.Fatalf("statement must end with ); got %.30s", line)
		}
		if strings.Contains(line, "NULL") {
			t.Fatal("full-fill rows must not render NULL")
		}
	}
	if total != 10 {
		t.Fatal("row count mismatch:", total)
	}
}

func TestSQLFormatPartialFillAndUpsert(t *testing.T) {
	// fill < file-cols: per-row random column sets break the groups early,
	// so statements hold fewer rows than the multiRow cap but the total row
	// count is preserved. Upsert statements carry the UPDATE clause.
	cfg := Config{TotalRows: 20, Segments: 1, Scatter: "none", Fields: 8, FixedCols: 0,
		IndexFields: 1, Fill: 3, RowWidth: 32, Seed: 3}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := bufio.NewWriter(&out)
	if err := g.WriteSQL(w, "t", 50, true); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	total := 0
	for _, line := range lines {
		if !strings.Contains(line, "ON DUPLICATE KEY UPDATE") {
			t.Fatal("upsert clause missing")
		}
		total += strings.Count(line, "),(") + 1
	}
	if total != 20 {
		t.Fatal("row count mismatch:", total)
	}
	// Determinism: same config+seed reproduces byte-identical output.
	g2, _ := NewGenerator(cfg, 0)
	var again bytes.Buffer
	w2 := bufio.NewWriter(&again)
	if err := g2.WriteSQL(w2, "t", 50, true); err != nil {
		t.Fatal(err)
	}
	w2.Flush()
	if !bytes.Equal(out.Bytes(), again.Bytes()) {
		t.Fatal("sql generation not deterministic")
	}
}

func TestSQLValidation(t *testing.T) {
	g, err := NewGenerator(Config{TotalRows: 2, Segments: 1, Scatter: "none", Fields: 2, Fill: 1, IndexFields: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.WriteSQL(bufio.NewWriter(&bytes.Buffer{}), "", 1, false); err == nil {
		t.Fatal("empty table accepted")
	}
	if err := g.WriteSQL(bufio.NewWriter(&bytes.Buffer{}), "t", 0, false); err == nil {
		t.Fatal("zero multi-row accepted")
	}
	args := []string{"-total-rows", "2", "-format", "sql", "-sql-multi-row", "2"}
	if err := Run(args); err == nil || !strings.Contains(err.Error(), "sql-table") {
		t.Fatal("missing sql-table accepted:", err)
	}
	if err := Run([]string{"-total-rows", "2", "-sql-table", "t"}); err == nil {
		t.Fatal("sql-table without -format sql accepted")
	}
}

func TestInvalidDimensions(t *testing.T) {
	for _, cfg := range []Config{
		{}, {TotalRows: 3, Segments: 2, Scatter: "none"},
		{TotalRows: 2, Segments: 1, Scatter: "none", Fields: -1},
		{TotalRows: 2, Segments: 1, Scatter: "none", Fields: 2, Fill: 3},
		{TotalRows: 1<<32 + 1, Segments: 1, Scatter: "hash32"},
	} {
		if _, err := NewGenerator(cfg, 0); err == nil {
			t.Fatalf("accepted invalid config %+v", cfg)
		}
	}
	if _, err := NewGenerator(Config{TotalRows: 2, Segments: 1, Scatter: "none"}, 0); err != nil {
		t.Fatal("zero-fill rows must be supported:", err)
	}
}

func TestIndexColsExplicit(t *testing.T) {
	cfg := Config{TotalRows: 8, Segments: 1, Scatter: "none", Fields: 10, Fill: 4,
		IndexCols: []int{1, 7}, IndexFields: 2, RowWidth: 32, Seed: 5}
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(0); seq < 8; seq++ {
		rec, err := g.Next(seq)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []int{1, 7} {
			if rec[1+c] == "" {
				t.Fatalf("index column %d empty at row %d", c, seq)
			}
		}
	}
}

func TestIndexColsValidation(t *testing.T) {
	cases := []Config{
		{TotalRows: 8, Segments: 1, Scatter: "none", Fields: 5, Fill: 2, IndexCols: []int{5}},
		{TotalRows: 8, Segments: 1, Scatter: "none", Fields: 5, Fill: 2, IndexCols: []int{1, 1}},
		{TotalRows: 8, Segments: 1, Scatter: "none", Fields: 5, Fill: 1, IndexCols: []int{1, 2}},
	}
	for _, cfg := range cases {
		if _, err := NewGenerator(cfg, 0); err == nil {
			t.Fatalf("expected error for index-cols %v", cfg.IndexCols)
		}
	}
}

func TestParseIndexCols(t *testing.T) {
	cols, err := parseIndexCols("c000001, c000003,4")
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 2, 3}
	if len(cols) != len(want) {
		t.Fatalf("got %v want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("got %v want %v", cols, want)
		}
	}
	if _, err := parseIndexCols("c0,abc"); err == nil {
		t.Fatal("expected error for invalid column")
	}
}

// collectKeySeqRows walks one keyseq segment in output order and returns the
// engine keys (hash, id pairs) plus the seqs that produced them.
func collectKeySeqRows(t *testing.T, cfg Config, seg uint64) ([][2]uint64, []uint64) {
	t.Helper()
	g, err := NewGenerator(cfg, seg)
	if err != nil {
		t.Fatal(err)
	}
	var keys [][2]uint64
	var seqs []uint64
	if err := g.forEachSeq(context.Background(), func(seq uint64) error {
		rec, err := g.Next(seq)
		if err != nil {
			return err
		}
		id, err := strconv.ParseUint(rec[0], 10, 64)
		if err != nil {
			return err
		}
		keys = append(keys, [2]uint64{uint64(genkey.EngineHash32(id)), id})
		seqs = append(seqs, seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return keys, seqs
}

func TestKeySeqScatter(t *testing.T) {
	cfg := Config{TotalRows: 200, Segments: 4, Scatter: "keyseq",
		Fields: 5, Fill: 3, IndexFields: 1, RowWidth: 32, Seed: 7}
	perSeg := cfg.RowsPerSeg()

	var allIDs []uint64
	for seg := uint64(0); seg < cfg.Segments; seg++ {
		keys, _ := collectKeySeqRows(t, cfg, seg)
		ids := make([]uint64, len(keys))
		for i, k := range keys {
			ids[i] = k[1]
		}
		if uint64(len(ids)) != perSeg {
			t.Fatalf("segment %d: got %d rows, want %d", seg, len(ids), perSeg)
		}
		for i := 1; i < len(keys); i++ {
			if keys[i-1][0] > keys[i][0] || (keys[i-1][0] == keys[i][0] && keys[i-1][1] >= keys[i][1]) {
				t.Fatalf("segment %d: engine keys not strictly ascending at row %d", seg, i)
			}
		}
		if seg > 0 {
			// keyseq's core property: segment engine-key ranges are disjoint and ordered.
			prev, _ := collectKeySeqRows(t, cfg, seg-1)
			if less(keys[0], prev[len(prev)-1]) {
				t.Fatalf("segment %d first key %v not above segment %d last key %v", seg, keys[0], seg-1, prev[len(prev)-1])
			}
		}
		allIDs = append(allIDs, ids...)

		// SegKeyBounds reports the exact id range that was emitted.
		g, err := NewGenerator(cfg, seg)
		if err != nil {
			t.Fatal(err)
		}
		minID, maxID := g.SegKeyBounds()
		if minID != ids[0] || maxID != ids[len(ids)-1] {
			t.Fatalf("segment %d bounds [%d,%d] do not bracket emitted ids [%d..%d]", seg, minID, maxID, ids[0], ids[len(ids)-1])
		}
	}

	// keyseq re-groups the same id set as hash32: same rows, different packing.
	hashCfg := cfg
	hashCfg.Scatter = "hash32"
	var hashIDs []uint64
	for seg := uint64(0); seg < hashCfg.Segments; seg++ {
		g, err := NewGenerator(hashCfg, seg)
		if err != nil {
			t.Fatal(err)
		}
		for seq := seg * perSeg; seq < (seg+1)*perSeg; seq++ {
			rec, err := g.Next(seq)
			if err != nil {
				t.Fatal(err)
			}
			id, err := strconv.ParseUint(rec[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			hashIDs = append(hashIDs, id)
		}
	}
	slices.Sort(allIDs)
	slices.Sort(hashIDs)
	if !slices.Equal(allIDs, hashIDs) {
		t.Fatal("keyseq id set differs from hash32 id set")
	}

	// Out-of-order calls are rejected: keyseq rows must arrive in engine key order.
	g, err := NewGenerator(cfg, 0)
	if err != nil {
		t.Fatal(err)
	}
	ks := g.keySeq
	if _, err := g.Next(ks.SeqAt(1)); err == nil {
		t.Fatal("expected out-of-key-order error")
	}
}

// less compares two engine keys (hash, id).
func less(a, b [2]uint64) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	return a[1] < b[1]
}

// TestKeySeqPairSystemValues: pair columns derive from the sequence, so the
// same sequence renders the same system values under keyseq and hash32; only
// the row packing differs.
func TestKeySeqPairSystemValues(t *testing.T) {
	base := Config{TotalRows: 40, Segments: 2, Scatter: "keyseq",
		Fields: 1, Fill: 1, IndexFields: 0, RowWidth: 32, Seed: 3,
		PairCatalog: 10, PairFill: 2, PairRawDist: []PairRaw{{Bytes: 16, Weight: 1.0}}, PairTimeSpan: 259200000,
		PairIndex: PairIndex{DWDistinct: 4, RowsPerPgID: 5, RowsPerOssPath: 2, RowsPerAnonymousID: 4, RowsPerAttachment: 2, RowsPerPackageName: 5, RowsPerParentID: 4}}
	probes := []struct{ seg, seq uint64 }{{0, 0}, {0, 1}, {0, 7}, {1, 20}, {1, 39}}
	systemCols := []string{"dwId", "createTime", "parentId", "dataType", "ms_dataUniqueId"}
	rendered := map[string]map[uint64]map[string]string{"keyseq": {}, "hash32": {}}
	for _, scatter := range []string{"keyseq", "hash32"} {
		gens := map[uint64]*Generator{}
		for _, seg := range []uint64{0, 1} {
			cfg := base
			cfg.Scatter = scatter
			g, err := NewGenerator(cfg, seg)
			if err != nil {
				t.Fatal(err)
			}
			gens[seg] = g
		}
		for _, p := range probes {
			cols, vals, err := gens[p.seg].NextPairs(p.seq)
			if err != nil {
				t.Fatalf("%s seq %d: %v", scatter, p.seq, err)
			}
			row := map[string]string{}
			for i, c := range cols {
				row[c] = vals[i]
			}
			rendered[scatter][p.seq] = row
		}
	}
	for _, p := range probes {
		for _, sys := range systemCols {
			a := rendered["keyseq"][p.seq][sys]
			b := rendered["hash32"][p.seq][sys]
			if a != b {
				t.Fatalf("seq %d %s: keyseq %q != hash32 %q", p.seq, sys, a, b)
			}
		}
	}
}

func TestPairRejectsExplicitGenericFlags(t *testing.T) {
	base := []string{"-total-rows", "10", "-segments", "1", "-format", "sql", "-sql-table", "t",
		"-pair-catalog", "100", "-pair-fill", "10", "-pair-raw-bytes", "96:1"}
	for _, name := range []string{"fields", "file-cols", "fixed-cols", "index-fields", "fill", "row-width"} {
		t.Run(name, func(t *testing.T) {
			args := append([]string(nil), base...)
			args = append(args, "-"+name, "1", "-out", t.TempDir()+"/seg")
			err := Run(args)
			if err == nil || !strings.Contains(err.Error(), "-"+name) || !strings.Contains(err.Error(), "pair mode") {
				t.Fatalf("explicit -%s was not rejected clearly: %v", name, err)
			}
		})
	}
}

func TestGapFactorRequiresBlockedScatter(t *testing.T) {
	err := Run([]string{"-total-rows", "10", "-segments", "1", "-scatter", "hash32", "-gap-factor", "5",
		"-out", t.TempDir() + "/seg"})
	if err == nil || !strings.Contains(err.Error(), "gap-factor") || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("gap-factor with hash32 was not rejected clearly: %v", err)
	}
}

// A pair-mode generator with per-row random column sets cannot group rows into
// one multi-row INSERT statement: every row gets a different column list, so
// the group breaks on the second row and each statement holds exactly one row.
// The flag used to be accepted and silently do nothing (the 2026-10-26 定稿轮
// was measured that way), so Run must now refuse the combination instead.
func TestPairMultiRowRequiresFillBuckets(t *testing.T) {
	base := []string{"-total-rows", "200", "-segments", "1",
		"-format", "sql", "-sql-table", "t", "-pair-catalog", "100", "-pair-fill", "10",
		"-pair-raw-bytes", "96:1"}
	cases := []struct {
		name    string
		flags   []string
		wantErr string
	}{
		{"multi-row without buckets", []string{"-sql-multi-row", "50"}, "fill-buckets"},
		{"multi-row with buckets", []string{"-sql-multi-row", "50", "-fill-buckets", "1"}, ""},
		{"single row without buckets", nil, ""},
		{"bucket-rows without buckets", []string{"-pair-bucket-rows", "50"}, "fill-buckets"},
		{"bucket-rows with buckets", []string{"-pair-bucket-rows", "50", "-fill-buckets", "1"}, ""},
		{"upsert in pair mode", []string{"-sql-upsert"}, "upsert"},
	}
	for _, c := range cases {
		// A fresh output path per case: publishing refuses to overwrite.
		args := append([]string(nil), base...)
		args = append(args, "-out", t.TempDir()+"/seg")
		err := Run(append(args, c.flags...))
		if c.wantErr == "" {
			if err != nil {
				t.Errorf("%s: refused a valid combination: %v", c.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted, want refusal mentioning %q", c.name, c.wantErr)
		} else if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: error %q does not mention %q", c.name, err, c.wantErr)
		}
	}
}

// -pair-bucket-rows only means something inside pair mode.
func TestPairBucketRowsOutsidePairMode(t *testing.T) {
	err := Run([]string{"-total-rows", "200", "-segments", "1", "-out", t.TempDir() + "/seg",
		"-format", "sql", "-sql-table", "t", "-pair-bucket-rows", "50"})
	if err == nil || !strings.Contains(err.Error(), "pair-catalog") {
		t.Fatalf("bucket-rows outside pair mode accepted: %v", err)
	}
}
