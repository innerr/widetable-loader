package gen

import (
	"bufio"
	"flag"
	"fmt"
	"os"
)

// Run parses flags and generates one segment file.
func Run(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	var (
		cfg     Config
		segment = fs.Uint64("segment", 0, "segment index to generate (0-based)")
		out     = fs.String("out", "", "output file (default stdout)")
	)
	fs.Uint64Var(&cfg.TotalRows, "total-rows", 0, "total rows across all segments")
	fs.Uint64Var(&cfg.Segments, "segments", 8, "total segment count")
	fs.StringVar(&cfg.Scatter, "scatter", "blocked", "key layout: blocked | none | mul-hash")
	fs.Uint64Var(&cfg.GapFactor, "gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor (gap is left for post-import writes)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed; regenerated data is byte-identical for the same seed and parameters")
	fs.IntVar(&cfg.Fields, "fields", 5000, "declared column count in the table schema (c000001..cN)")
	fs.IntVar(&cfg.FileCols, "file-cols", 0, "declared columns present in the output file (default = fields); a smaller column set cuts the per-row delimiter overhead of empty columns")
	fs.IntVar(&cfg.FixedCols, "fixed-cols", 6, "fixed column count (always filled)")
	fs.IntVar(&cfg.IndexFields, "index-fields", 15, "declared columns carrying a secondary index (filled on every row)")
	fs.IntVar(&cfg.Fill, "fill", 100, "filled declared columns per row (index fields included)")
	fs.IntVar(&cfg.RowWidth, "row-width", 1024, "target row width in bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.TotalRows == 0 {
		return fmt.Errorf("--total-rows is required")
	}

	g, err := NewGenerator(cfg, *segment)
	if err != nil {
		return err
	}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	// Sidecar column list for the loader's LOAD DATA / INSERT statements:
	// one column name per line, in file order.
	if *out != "" {
		cf, err := os.Create(*out + ".columns")
		if err != nil {
			return err
		}
		defer cf.Close()
		for _, name := range cfg.ColumnNames() {
			fmt.Fprintln(cf, name)
		}
	}

	buf := bufio.NewWriterSize(w, 1<<20)
	row0 := *segment * cfg.RowsPerSeg()
	row1 := row0 + cfg.RowsPerSeg()
	for seq := row0; seq < row1; seq++ {
		record, err := g.Next(seq)
		if err != nil {
			return err
		}
		writeRecord(buf, record)
	}
	return buf.Flush()
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
