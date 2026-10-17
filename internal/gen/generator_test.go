package gen

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
