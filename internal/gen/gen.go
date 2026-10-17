package gen

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Run parses flags and generates one segment file.
func Run(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	var (
		cfg         Config
		segment     = fs.Uint64("segment", 0, "segment index to generate (0-based)")
		out         = fs.String("out", "", "output file (default stdout)")
		format      = fs.String("format", "csv", "output format: csv | sql")
		sqlTable    = fs.String("sql-table", "", "sql format: target table name for INSERT statements")
		sqlMultiRow = fs.Int("sql-multi-row", 1, "sql format: rows per INSERT statement (groups break earlier when the filled-column set changes)")
		sqlUpsert   = fs.Bool("sql-upsert", false, "sql format: append ON DUPLICATE KEY UPDATE")
	)
	fs.Uint64Var(&cfg.TotalRows, "total-rows", 0, "total rows across all segments")
	fs.Uint64Var(&cfg.Segments, "segments", 8, "total segment count")
	fs.StringVar(&cfg.Scatter, "scatter", "hash32", "key layout: hash32 (customer scatter, default) | blocked | none | mul-hash")
	fs.Uint64Var(&cfg.GapFactor, "gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor (gap is left for post-import writes)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed; regenerated data is byte-identical for the same seed and parameters")
	fs.IntVar(&cfg.Fields, "fields", 5000, "declared column count in the table schema (c000001..cN)")
	fs.IntVar(&cfg.FileCols, "file-cols", 0, "declared columns present in the output file (default = fields); a smaller column set cuts the per-row delimiter overhead of empty columns")
	fs.IntVar(&cfg.FixedCols, "fixed-cols", 6, "fixed column count (always filled)")
	fs.IntVar(&cfg.IndexFields, "index-fields", 15, "declared columns carrying a secondary index (filled on every row)")
	fs.IntVar(&cfg.Fill, "fill", 100, "filled declared columns per row (index fields included)")
	fs.IntVar(&cfg.RowWidth, "row-width", 1024, "target row width in bytes")
	fs.IntVar(&cfg.FillBuckets, "fill-buckets", 0, ">0: cycle through N pre-picked column sets instead of per-row random (enables multi-row INSERT)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.TotalRows == 0 {
		return fmt.Errorf("--total-rows is required")
	}
	if *format != "csv" && *format != "sql" {
		return fmt.Errorf("unknown format %q (csv | sql)", *format)
	}
	sql := *format == "sql"
	if !sql && (*sqlTable != "" || *sqlMultiRow != 1 || *sqlUpsert) {
		return fmt.Errorf("--sql-table/--sql-multi-row/--sql-upsert require -format sql")
	}
	if sql && *sqlTable == "" {
		return fmt.Errorf("-format sql requires --sql-table")
	}
	if *sqlMultiRow < 1 {
		return fmt.Errorf("--sql-multi-row must be >= 1")
	}

	g, err := NewGenerator(cfg, *segment)
	if err != nil {
		return err
	}

	w := os.Stdout
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			return err
		}
		ext := ".csv"
		if sql {
			ext = ".sql"
		}
		f, err := os.CreateTemp(filepath.Dir(*out), ".generation-*"+ext)
		if err != nil {
			return err
		}
		defer f.Close()
		defer os.Remove(f.Name())
		w = f
		if sql {
			if err := g.WriteSQL(bufio.NewWriterSize(f, 1<<20), *sqlTable, *sqlMultiRow, *sqlUpsert); err != nil {
				return err
			}
		} else {
			defer os.Remove(f.Name() + ".columns")
			if err := g.WriteCSV(bufio.NewWriterSize(f, 1<<20)); err != nil {
				return err
			}
			if err := os.WriteFile(f.Name()+".columns", []byte(strings.Join(cfg.ColumnNames(), "\n")+"\n"), 0o644); err != nil {
				return err
			}
		}
		if err := f.Close(); err != nil {
			return err
		}
		return publishSegment(f.Name(), *out, !sql)
	}
	if sql {
		return g.WriteSQL(bufio.NewWriterSize(w, 1<<20), *sqlTable, *sqlMultiRow, *sqlUpsert)
	}
	buf := bufio.NewWriterSize(w, 1<<20)
	if err := g.WriteCSV(buf); err != nil {
		return err
	}
	return buf.Flush()
}

// publishSegment never overwrites data or the column list from an earlier
// run; the column sidecar is only linked for csv segments (sql statements
// carry their own column lists).
func publishSegment(stage, out string, columns bool) error {
	if columns {
		if err := os.Link(stage+".columns", out+".columns"); err != nil {
			return err
		}
	}
	if err := os.Link(stage, out); err != nil {
		os.Remove(out + ".columns")
		return err
	}
	return nil
}

// PublishSegment is the exported publish path for daemon-driven generation.
func PublishSegment(stage, out string, columns bool) error {
	return publishSegment(stage, out, columns)
}

// WriteCSV writes every row of the generator's segment as CSV.
func (g *Generator) WriteCSV(w *bufio.Writer) error {
	return g.WriteCSVContext(context.Background(), w)
}

func (this *Generator) WriteCSVContext(ctx context.Context, w *bufio.Writer) error {
	for seq := this.row0; seq < this.row1; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := this.Next(seq)
		if err != nil {
			return err
		}
		writeRecord(w, record)
	}
	return w.Flush()
}

// writeRecord writes one row: empty fields as \N (SQL NULL), non-empty
// fields double-quoted with inner quotes doubled. Field values never
// contain commas or newlines, so this parse-matches
// FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'.
func writeRecord(buf *bufio.Writer, record []string) {
	for i, v := range record {
		if i > 0 {
			buf.WriteByte(',')
		}
		if v == "" {
			buf.WriteString(`\N`)
			continue
		}
		buf.WriteByte('"')
		for j := 0; j < len(v); j++ {
			if v[j] == '"' {
				buf.WriteByte('"')
			}
			buf.WriteByte(v[j])
		}
		buf.WriteByte('"')
	}
	buf.WriteByte('\n')
}
