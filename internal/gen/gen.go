// Package gen implements the "gen" subcommand: synthetic wide-table data
// generation, one CSV file per key segment.
//
// Row model (generic, parameter-driven):
//
//	id (BIGINT primary key) | f1..fF (fixed columns, always filled) |
//	c000001..cN (declared columns, sparsely filled)
//
// The first --index-fields declared columns are filled on every row
// (they carry secondary indexes); the remaining filled columns are chosen
// uniformly at random. Declared column types rotate through
// int / string / double / bool / json.
package gen

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/innerr/widetable-loader/internal/genkey"
)

const letters = "abcdefghijklmnopqrstuvwxyz0123456789"

// Run parses flags and generates one segment file.
func Run(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	var (
		totalRows   = fs.Uint64("total-rows", 0, "total rows across all segments")
		segments    = fs.Uint64("segments", 8, "total segment count")
		segment     = fs.Uint64("segment", 0, "segment index to generate (0-based)")
		scatter     = fs.String("scatter", "blocked", "key layout: blocked | none | mul-hash")
		gapFactor   = fs.Uint64("gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor (gap is left for post-import writes)")
		seed        = fs.Uint64("seed", 1, "random seed; regenerated data is byte-identical for the same seed and parameters")
		fields      = fs.Int("fields", 5000, "declared column count in the table schema (c000001..cN)")
		poolSize    = fs.Int("pool-size", 0, "declared columns present in the output file (default = fields); a smaller pool cuts the per-row delimiter overhead of empty columns")
		fixedCols   = fs.Int("fixed-cols", 6, "fixed column count (always filled)")
		indexFields = fs.Int("index-fields", 15, "declared columns carrying a secondary index (filled on every row)")
		fill        = fs.Int("fill", 100, "filled declared columns per row (index fields included)")
		rowWidth    = fs.Int("row-width", 1024, "target row width in bytes")
		out         = fs.String("out", "", "output file (default stdout)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *totalRows == 0 {
		return fmt.Errorf("--total-rows is required")
	}
	if *segment >= *segments {
		return fmt.Errorf("--segment must be < --segments")
	}
	if *fill > *fields || *indexFields > *fill {
		return fmt.Errorf("require index-fields <= fill <= fields")
	}
	if *poolSize == 0 {
		*poolSize = *fields
	}
	if *poolSize > *fields || *fill > *poolSize {
		return fmt.Errorf("require fill <= pool-size <= fields")
	}

	rowsPerSeg := *totalRows / *segments
	row0 := *segment * rowsPerSeg       // first global sequence of this segment
	row1 := row0 + rowsPerSeg           // one past the last
	stride := rowsPerSeg                // segment base stride
	if *scatter == "blocked" {
		stride = rowsPerSeg * *gapFactor
	}

	var layout genkey.Layout
	switch *scatter {
	case "blocked", "none":
		layout = genkey.Identity{Base: *segment * stride}
	case "mul-hash":
		layout = genkey.NewMulHash(*seed)
	default:
		return fmt.Errorf("unknown --scatter %q (blocked | none | mul-hash)", *scatter)
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
	cw := csv.NewWriter(w)
	defer cw.Flush()

	// Sidecar column list for the loader's LOAD DATA / INSERT statements:
	// one column name per line, in file order.
	if *out != "" {
		cf, err := os.Create(*out + ".columns")
		if err != nil {
			return err
		}
		defer cf.Close()
		fmt.Fprintln(cf, "id")
		for c := 1; c <= *fixedCols; c++ {
			fmt.Fprintf(cf, "f%d\n", c)
		}
		for c := 1; c <= *poolSize; c++ {
			fmt.Fprintf(cf, "c%06d\n", c)
		}
	}

	rng := rand.New(rand.NewSource(int64(*seed*1000003 + *segment)))
	// Long values are carried by string/json columns only (2 of 5 rotated
	// types), so spread the target width over them, not over all filled
	// columns.
	strLen := (*rowWidth * 5) / (*fill * 2)
	if strLen < 8 {
		strLen = 8
	}

	// Column types rotate: 0 int, 1 string, 2 double, 3 bool, 4 json.
	// The file carries id + fixed columns + the first pool declared columns.
	record := make([]string, 1+*fixedCols+*poolSize)
	perm := make([]int, *poolSize-*indexFields)
	for i := range perm {
		perm[i] = *indexFields + i
	}

	for seq := row0; seq < row1; seq++ {
		for i := range record {
			record[i] = ""
		}
		record[0] = strconv.FormatUint(layout.ID(seq), 10)
		for c := 1; c <= *fixedCols; c++ {
			record[c] = fixedValue(c, seq)
		}
		// Index columns are always filled.
		for c := 0; c < *indexFields; c++ {
			record[1+*fixedCols+c] = value(c, seq, strLen, rng)
		}
		// The rest: partial Fisher-Yates to pick fill-indexFields columns.
		need := *fill - *indexFields
		for i := 0; i < need; i++ {
			j := i + rng.Intn(len(perm)-i)
			perm[i], perm[j] = perm[j], perm[i]
			record[1+*fixedCols+perm[i]] = value(perm[i], seq, strLen, rng)
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// fixedValue fills fixed columns by position: c1 increments like a
// timestamp, c2 is a constant zero flag, the rest cycle cheaply.
func fixedValue(col int, seq uint64) string {
	switch col {
	case 1:
		return strconv.FormatUint(1700000000+seq, 10)
	case 2:
		return "0"
	default:
		return fmt.Sprintf("f%d-%d", col, seq%997)
	}
}

// value renders a column value by its rotated type. String and json columns
// carry the row-width payload; numeric columns stay short.
func value(col int, seq uint64, strLen int, rng *rand.Rand) string {
	switch col % 5 {
	case 0:
		return strconv.FormatUint(seq%1000000, 10)
	case 2:
		return strconv.FormatFloat(float64(seq%1000000)/97.0, 'f', 2, 64)
	case 3:
		if seq%2 == 0 {
			return "true"
		}
		return "false"
	case 4:
		return `{"k":"` + randString(strLen, rng) + `"}`
	default:
		return randString(strLen, rng)
	}
}

func randString(n int, rng *rand.Rand) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
