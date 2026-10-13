// Package check implements the "check" subcommand: post-import data
// verification against regenerated expected rows.
//
// Three checks, in order:
//  1. row count: SELECT COUNT(*) vs --total-rows;
//  2. sampled rows: regenerate sampled rows from --seed and compare the
//     filled-column set and exact values of string/int/fixed columns
//     (double/bool/json columns are compared for presence only: their SQL
//     text representation is not byte-stable);
//  3. index probe: point-query the first indexed declared column of each
//     sampled row and require the expected id in the returned set.
//
// Sampling is deterministic (--seed): a re-run checks the same rows.
package check

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/innerr/widetable-loader/internal/gen"
)

// Run parses flags and verifies the table against regenerated rows.
func Run(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	var (
		dsn     = fs.String("dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:port)/db")
		table   = fs.String("table", "", "target table")
		cfg     gen.Config
		sampleK = fs.Int("sample-per-segment", 8, "sampled rows per segment")
	)
	fs.Uint64Var(&cfg.TotalRows, "total-rows", 0, "total rows across all segments (must match gen)")
	fs.Uint64Var(&cfg.Segments, "segments", 8, "total segment count (must match gen)")
	fs.StringVar(&cfg.Scatter, "scatter", "blocked", "key layout (must match gen)")
	fs.Uint64Var(&cfg.GapFactor, "gap-factor", 10, "blocked gap factor (must match gen)")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed (must match gen)")
	fs.IntVar(&cfg.Fields, "fields", 5000, "declared column count (must match gen)")
	fs.IntVar(&cfg.FileCols, "file-cols", 0, "declared columns in the load file (must match gen)")
	fs.IntVar(&cfg.FixedCols, "fixed-cols", 6, "fixed column count (must match gen)")
	fs.IntVar(&cfg.IndexFields, "index-fields", 15, "indexed declared columns (must match gen)")
	fs.IntVar(&cfg.Fill, "fill", 100, "filled declared columns per row (must match gen)")
	fs.IntVar(&cfg.RowWidth, "row-width", 1024, "target row width (must match gen)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" || *table == "" || cfg.TotalRows == 0 {
		return fmt.Errorf("--dsn, --table and --total-rows are required")
	}
	if cfg.FileCols == 0 {
		cfg.FileCols = cfg.Fields
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	fails := 0

	// 1. Row count.
	var cnt uint64
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", *table)).Scan(&cnt); err != nil {
		return fmt.Errorf("count query: %w", err)
	}
	if cnt != cfg.TotalRows {
		fmt.Printf("FAIL row count: got %d, want %d\n", cnt, cfg.TotalRows)
		fails++
	} else {
		fmt.Printf("PASS row count: %d\n", cnt)
	}

	colNames := cfg.ColumnNames()
	colList := "`" + strings.Join(colNames, "`,`") + "`"

	// 2+3. Per-segment sampling.
	for seg := uint64(0); seg < cfg.Segments; seg++ {
		g, err := gen.NewGenerator(cfg, seg)
		if err != nil {
			return err
		}
		row0 := seg * cfg.RowsPerSeg()
		rowsPerSeg := cfg.RowsPerSeg()

		// Deterministic distinct sample offsets within the segment.
		srng := rand.New(rand.NewSource(int64(cfg.Seed*7919 + seg)))
		k := *sampleK
		if uint64(k) > rowsPerSeg {
			k = int(rowsPerSeg)
		}
		offSet := map[uint64]struct{}{}
		for len(offSet) < k {
			offSet[uint64(srng.Int63n(int64(rowsPerSeg)))] = struct{}{}
		}
		offs := make([]uint64, 0, k)
		for o := range offSet {
			offs = append(offs, o)
		}
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })

		// Single pass: advance the generator to each sampled row.
		expected := map[uint64][]string{} // offset -> record copy
		next := uint64(0)
		for _, off := range offs {
			for ; next <= off; next++ {
				rec, err := g.Next(row0 + next)
				if err != nil {
					return err
				}
				if next == off {
					cp := make([]string, len(rec))
					copy(cp, rec)
					expected[off] = cp
				}
			}
		}

		for _, off := range offs {
			rec := expected[off]
			ok := checkRow(db, *table, colList, colNames, cfg, rec)
			if !ok {
				fails++
			}
			if cfg.IndexFields > 0 {
				if !probeIndex(db, *table, cfg, rec) {
					fails++
				}
			}
		}
		fmt.Printf("segment %d: %d sampled rows checked\n", seg, k)
	}

	if fails > 0 {
		return fmt.Errorf("check FAILED: %d failing checks", fails)
	}
	fmt.Println("check PASSED")
	return nil
}

// checkRow compares one expected record against the stored row.
// record layout: [id, f1..fF, c000001..cFileCols].
func checkRow(db *sql.DB, table, colList string, colNames []string, cfg gen.Config, rec []string) bool {
	id := rec[0]
	rows, err := db.Query(fmt.Sprintf("SELECT %s FROM `%s` WHERE id = %s", colList, table, id))
	if err != nil {
		fmt.Printf("FAIL id %s: query: %v\n", id, err)
		return false
	}
	defer rows.Close()
	if !rows.Next() {
		fmt.Printf("FAIL id %s: row missing\n", id)
		return false
	}
	vals := make([]sql.NullString, len(colNames))
	ptrs := make([]interface{}, len(colNames))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		fmt.Printf("FAIL id %s: scan: %v\n", id, err)
		return false
	}

	ok := true
	for i, name := range colNames {
		want := rec[i]
		got := ""
		if vals[i].Valid {
			got = vals[i].String
		}
		if (want == "") != (got == "") {
			fmt.Printf("FAIL id %s col %s: presence mismatch (want filled=%v, got filled=%v)\n", id, name, want != "", got != "")
			ok = false
			continue
		}
		if want == "" {
			continue
		}
		if exactCompare(name, i, cfg) && want != got {
			fmt.Printf("FAIL id %s col %s: value mismatch (want %.40q, got %.40q)\n", id, name, want, got)
			ok = false
		}
	}
	return ok
}

// exactCompare reports whether the column value is byte-stable through SQL.
// Fixed columns and int/string declared columns are exact; double/bool/json
// representations are not (compared for presence only).
func exactCompare(name string, idx int, cfg gen.Config) bool {
	if idx <= cfg.FixedCols {
		return true // id and fixed columns
	}
	declaredIdx := idx - 1 - cfg.FixedCols // 0-based declared column index
	return declaredIdx%5 == 0 || declaredIdx%5 == 1 // int or string
}

// probeIndex point-queries the first indexed declared column and requires
// the expected id in the returned set (the value is not unique).
func probeIndex(db *sql.DB, table string, cfg gen.Config, rec []string) bool {
	idx := 1 + cfg.FixedCols // record index of c000001
	val := rec[idx]
	id := rec[0]
	rows, err := db.Query(fmt.Sprintf("SELECT id FROM `%s` WHERE c000001 = '%s'", table, val))
	if err != nil {
		fmt.Printf("FAIL index probe id %s: query: %v\n", id, err)
		return false
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var got string
		if err := rows.Scan(&got); err != nil {
			fmt.Printf("FAIL index probe id %s: scan: %v\n", id, err)
			return false
		}
		if got == id {
			found = true
		}
	}
	if !found {
		fmt.Printf("FAIL index probe id %s: c000001=%.40q does not reach the row\n", id, val)
		return false
	}
	return true
}
