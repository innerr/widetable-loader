package gen

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/innerr/widetable-loader/internal/genkey"
)

// Run parses flags and generates one segment file. The special form
// Run(["keyseq-bounds", ...]) writes the segment boundary table of a keyseq
// dataset (the primary-key range of every segment) as JSON, for use as
// explicit presplit keys.
func Run(args []string) error {
	if len(args) > 0 && args[0] == "keyseq-bounds" {
		return runKeySeqBounds(args[1:])
	}
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	var (
		cfg         Config
		segment     = fs.Uint64("segment", 0, "segment index to generate (0-based)")
		out         = fs.String("out", "", "output file (default stdout)")
		format      = fs.String("format", "csv", "output format: csv | sql")
		sqlTable    = fs.String("sql-table", "", "sql format: target table name for INSERT statements")
		sqlMultiRow = fs.Int("sql-multi-row", 1, "sql format: rows per INSERT statement (groups break earlier when the filled-column set changes)")
		sqlUpsert   = fs.Bool("sql-upsert", false, "sql format: append ON DUPLICATE KEY UPDATE")
		indexCols   = fs.String("index-cols", "", "comma-separated declared columns carrying secondary indexes, e.g. c000001,c000003; overrides -index-fields")
	)
	fs.BoolVar(&cfg.WriteHashSidecar, "write-hash-sidecar", false,
		"write each segment's content hash next to it, so a later import can skip hashing the dataset; off by default (matches earlier rounds)")
	fs.Uint64Var(&cfg.TotalRows, "total-rows", 0, "total rows across all segments")
	fs.Uint64Var(&cfg.Segments, "segments", 8, "total segment count")
	fs.StringVar(&cfg.Scatter, "scatter", "hash32", "key layout: hash32 (customer scatter, default) | keyseq (key-ordered segments with disjoint ranges) | blocked | none | mul-hash")
	fs.Uint64Var(&cfg.GapFactor, "gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor (gap is left for post-import writes)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed; regenerated data is byte-identical for the same seed and parameters")
	fs.IntVar(&cfg.Fields, "fields", 5000, "declared column count in the table schema (c000001..cN)")
	fs.IntVar(&cfg.FileCols, "file-cols", 0, "declared columns present in the output file (default = fields); a smaller column set cuts the per-row delimiter overhead of empty columns")
	fs.IntVar(&cfg.FixedCols, "fixed-cols", 6, "fixed column count (always filled)")
	fs.IntVar(&cfg.IndexFields, "index-fields", 15, "declared columns carrying a secondary index (filled on every row)")
	fs.IntVar(&cfg.Fill, "fill", 100, "filled declared columns per row (index fields included)")
	fs.IntVar(&cfg.RowWidth, "row-width", 1024, "target row width in bytes")
	fs.IntVar(&cfg.FillBuckets, "fill-buckets", 0, ">0: cycle through N pre-picked column sets instead of per-row random (enables multi-row INSERT)")
	pairRawSpec := RegisterPairFlags(fs, &cfg)
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
	if *indexCols != "" {
		cols, err := parseIndexCols(*indexCols)
		if err != nil {
			return err
		}
		cfg.IndexCols = cols
		cfg.IndexFields = len(cols)
	}
	if cfg.PairCatalog > 0 {
		if !sql {
			return fmt.Errorf("pair mode writes per-row column lists and requires -format sql")
		}
		spec, err := ParsePairRawDist(*pairRawSpec)
		if err != nil {
			return err
		}
		cfg.PairRawDist = spec
		// A multi-row INSERT statement has one column list, so the rows in a
		// group must fill exactly the same columns. With per-row random fills
		// (fill-buckets 0) the second row always breaks the group, so the flag
		// would silently do nothing and every statement ends up holding one row.
		// Refuse instead of degrading quietly.
		if *sqlMultiRow > 1 && cfg.FillBuckets == 0 {
			return fmt.Errorf("-sql-multi-row %d has no effect in pair mode without -fill-buckets: "+
				"per-row random column sets break the group on the second row, so every statement holds one row; "+
				"set -fill-buckets 1 to actually group %d rows per statement, or drop -sql-multi-row",
				*sqlMultiRow, *sqlMultiRow)
		}
		// Bucket rows only matter when buckets exist, and cycling buckets per
		// row defeats grouping -- the value below 1 changes nothing.
		if cfg.PairBucketRows != 0 && cfg.FillBuckets == 0 {
			return fmt.Errorf("-pair-bucket-rows %d has no effect without -fill-buckets: "+
				"column sets are picked per row, so there is no bucket to spread rows across", cfg.PairBucketRows)
		}
		if cfg.PairBucketRows < 0 {
			return fmt.Errorf("-pair-bucket-rows must be nonnegative")
		}
		if sql && *sqlUpsert {
			return fmt.Errorf("-sql-upsert is not supported in pair mode: pair output builds its own column list per statement")
		}
	} else if cfg.PairBucketRows != 0 {
		return fmt.Errorf("-pair-bucket-rows requires -pair-catalog (pair mode is off)")
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
		if sql && cfg.PairCatalog > 0 {
			if err := g.WriteSQLPairsMultiRow(bufio.NewWriterSize(f, 1<<20), *sqlTable, *sqlMultiRow); err != nil {
				return err
			}
		} else if sql {
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
		return publishSegment(f.Name(), *out, !sql, cfg.WriteHashSidecar)
	}
	if sql {
		if cfg.PairCatalog > 0 {
			return g.WriteSQLPairsMultiRow(bufio.NewWriterSize(w, 1<<20), *sqlTable, *sqlMultiRow)
		}
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
// carry their own column lists). With writeHash it also writes the segment's
// content hash next to it, so a later import can read the hash instead of
// hashing the segment (and its column sidecar) again before the first row is
// written; this is off by default.
func publishSegment(stage, out string, columns bool, writeHash bool) error {
	if columns {
		if err := os.Link(stage+".columns", out+".columns"); err != nil {
			return err
		}
	}
	if err := os.Link(stage, out); err != nil {
		os.Remove(out + ".columns")
		return err
	}
	if !writeHash {
		return nil
	}
	if err := WriteHashSidecar(out, columns); err != nil {
		os.Remove(out)
		if columns {
			os.Remove(out + ".columns")
		}
		return err
	}
	return nil
}

// PublishSegment is the exported publish path for daemon-driven generation.
func PublishSegment(stage, out string, columns bool, writeHash bool) error {
	return publishSegment(stage, out, columns, writeHash)
}

// WriteCSV writes every row of the generator's segment as CSV.
func (g *Generator) WriteCSV(w *bufio.Writer) error {
	return g.WriteCSVContext(context.Background(), w)
}

func (this *Generator) WriteCSVContext(ctx context.Context, w *bufio.Writer) error {
	return this.forEachSeq(ctx, func(seq uint64) error {
		record, err := this.Next(seq)
		if err != nil {
			return err
		}
		writeRecord(w, record)
		return nil
	})
}

// runKeySeqBounds emits the key range of every keyseq segment as one JSON
// object; segments are consecutive ranks of the global key order, so the
// max_id of segment i is below the min_id of segment i+1.
func runKeySeqBounds(args []string) error {
	fs := flag.NewFlagSet("gen keyseq-bounds", flag.ContinueOnError)
	totalRows := fs.Uint64("total-rows", 0, "total rows across all segments")
	segments := fs.Uint64("segments", 0, "total segment count")
	out := fs.String("out", "", "output file (default stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *totalRows == 0 || *segments == 0 || *totalRows%*segments != 0 {
		return fmt.Errorf("--total-rows and --segments are required, rows divisible by segments")
	}
	if *totalRows > 1<<32 {
		return fmt.Errorf("keyseq supports at most 2^32 rows")
	}
	rowsPerSeg := *totalRows / *segments
	ks := genkey.BuildKeySeq(*totalRows, genkey.Hash32Prefix{}.ID)
	type segBound struct {
		Segment uint64 `json:"segment"`
		MinID   uint64 `json:"min_id"`
		MaxID   uint64 `json:"max_id"`
		MinHash uint32 `json:"min_hash32"`
		MaxHash uint32 `json:"max_hash32"`
		Rows    uint64 `json:"rows"`
	}
	doc := struct {
		Scatter        string     `json:"scatter"`
		TotalRows      uint64     `json:"total_rows"`
		Segments       uint64     `json:"segments"`
		RowsPerSegment uint64     `json:"rows_per_segment"`
		Bounds         []segBound `json:"bounds"`
	}{
		Scatter:        "keyseq",
		TotalRows:      *totalRows,
		Segments:       *segments,
		RowsPerSegment: rowsPerSeg,
		Bounds:         make([]segBound, 0, *segments),
	}
	for s := uint64(0); s < *segments; s++ {
		minID, maxID := ks.SegmentBounds(s, rowsPerSeg)
		doc.Bounds = append(doc.Bounds, segBound{Segment: s, MinID: minID, MaxID: maxID,
			MinHash: ks.HashAt(s * rowsPerSeg), MaxHash: ks.HashAt((s+1)*rowsPerSeg - 1), Rows: rowsPerSeg})
	}
	blob, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	blob = append(blob, '\n')
	if *out == "" {
		_, err = os.Stdout.Write(blob)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(*out, blob, 0o644)
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

// parseIndexCols reads a comma-separated list of declared column names (c000001)
// or column numbers into 0-based column indexes.
func parseIndexCols(raw string) ([]int, error) {
	cols := make([]int, 0, 8)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		digits := strings.TrimLeft(part, "cC")
		n, err := strconv.Atoi(digits)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("index-cols entry %q is not a positive column number", part)
		}
		cols = append(cols, n-1)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("index-cols is empty")
	}
	return cols, nil
}
