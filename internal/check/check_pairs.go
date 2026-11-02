package check

// Pair-mode verification: every row fills a different column subset, so the
// fixed column list used by the declared-column check does not apply. Each
// sampled row is queried with the exact columns the generator produced, and
// one index probe uses the unique dataUniqueId.

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/sqlutil"
)

func runPairCheck(db *sql.DB, table string, cfg gen.Config, sampleK int) error {
	fails := 0
	var cnt uint64
	if err := db.QueryRow("SELECT COUNT(*) FROM " + sqlutil.QuoteIdentifier(table)).Scan(&cnt); err != nil {
		return fmt.Errorf("count query: %w", err)
	}
	if cnt != cfg.TotalRows {
		fmt.Printf("FAIL row count: got %d, want %d\n", cnt, cfg.TotalRows)
		fails++
	} else {
		fmt.Printf("PASS row count: %d\n", cnt)
	}
	for seg := uint64(0); seg < cfg.Segments; seg++ {
		g, err := gen.NewGenerator(cfg, seg)
		if err != nil {
			return err
		}
		row0 := seg * cfg.RowsPerSeg()
		rowsPerSeg := cfg.RowsPerSeg()
		srng := rand.New(rand.NewSource(int64(cfg.Seed*7919 + seg)))
		k := sampleK
		if uint64(k) > rowsPerSeg {
			k = int(rowsPerSeg)
		}
		seen := map[uint64]struct{}{}
		for len(seen) < k {
			seen[uint64(srng.Int63n(int64(rowsPerSeg)))] = struct{}{}
		}
		offs := make([]uint64, 0, k)
		for off := range seen {
			offs = append(offs, off)
		}
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		// One pass over the segment: the generator advances its column picker
		// per row, so skipping rows here would desynchronize it from the file
		// that was imported.
		type pair struct{ cols, vals []string }
		expected := map[uint64]pair{}
		next := uint64(0)
		for _, off := range offs {
			for ; next <= off; next++ {
				cols, vals, err := g.NextPairs(row0 + next)
				if err != nil {
					return err
				}
				if next == off {
					expected[off] = pair{append([]string(nil), cols...), append([]string(nil), vals...)}
				}
			}
		}
		for _, off := range offs {
			row := expected[off]
			if !checkPairRow(db, table, row.cols, row.vals) {
				fails++
			}
			if !probePairIndex(db, table, row.cols, row.vals) {
				fails++
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

// checkPairRow compares every generated column of one row against the stored
// row. Only the generated columns are selected; the stored row also carries
// columns this generator does not write.
func checkPairRow(db *sql.DB, table string, cols, vals []string) bool {
	quoted := make([]string, len(cols))
	for i, name := range cols {
		quoted[i] = sqlutil.QuoteIdentifier(name)
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE id = ?",
		strings.Join(quoted, ","), sqlutil.QuoteIdentifier(table))
	row := db.QueryRow(query, strings.Trim(vals[0], "'"))
	stored := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range stored {
		dest[i] = &stored[i]
	}
	if err := row.Scan(dest...); err != nil {
		fmt.Printf("FAIL id %s: row missing or query failed: %v\n", vals[0], err)
		return false
	}
	for i, name := range cols {
		if !stored[i].Valid {
			// A generated column that reads back NULL means the value was lost.
			fmt.Printf("FAIL id %s column %s: stored NULL, want %s\n", vals[0], name, vals[i])
			return false
		}
		if !pairValueEqual(vals[i], stored[i].String) {
			fmt.Printf("FAIL id %s column %s: got %q, want %q\n",
				vals[0], name, stored[i].String, vals[i])
			return false
		}
	}
	return true
}

// probePairIndex looks the row up by its unique dataUniqueId, which every row
// carries and which the customer indexes.
func probePairIndex(db *sql.DB, table string, cols, vals []string) bool {
	var id, unique string
	for i, name := range cols {
		switch name {
		case "id":
			id = strings.Trim(vals[i], "'")
		case "ms_dataUniqueId":
			unique = strings.Trim(vals[i], "'")
		}
	}
	if id == "" || unique == "" {
		return true
	}
	rows, err := db.Query(fmt.Sprintf(
		"SELECT id FROM %s WHERE ms_dataUniqueId = ?", sqlutil.QuoteIdentifier(table)), unique)
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
	if err := rows.Err(); err != nil {
		fmt.Printf("FAIL index probe id %s: rows: %v\n", id, err)
		return false
	}
	if !found {
		fmt.Printf("FAIL index probe id %s: dataUniqueId %s did not return this row\n", id, unique)
	}
	return found
}

// pairValueEqual compares a generated SQL literal with the stored text.
func pairValueEqual(want, got string) bool {
	want = strings.Trim(want, "'")
	if want == got {
		return true
	}
	if want == "true" || want == "false" {
		expected, err := strconv.ParseBool(want)
		if err != nil {
			return false
		}
		switch got {
		case "1":
			return expected
		case "0":
			return !expected
		}
		actual, err := strconv.ParseBool(got)
		return err == nil && actual == expected
	}
	wantNum, wantErr := strconv.ParseFloat(want, 64)
	gotNum, gotErr := strconv.ParseFloat(got, 64)
	if wantErr == nil && gotErr == nil {
		return math.Abs(wantNum-gotNum) < 1e-9
	}
	return false
}
