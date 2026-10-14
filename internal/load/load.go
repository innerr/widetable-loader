// Package load implements the "load" subcommand: concurrent segment loading
// via LOAD DATA (csv entry) or multi-row INSERT (insert entry).
//
// Bulk-load prerequisites (the tool does not verify them):
//  1. the target table is empty before the import task starts;
//  2. primary keys are unique across the whole task;
//  3. no other writes happen against the table during the import.
package load

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Run parses flags and loads all segment files with a worker pool.
func Run(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	var (
		dsn       = fs.String("dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:port)/db")
		table     = fs.String("table", "", "target table")
		dir       = fs.String("segments-dir", "", "directory holding segment CSV files")
		workers   = fs.Int("workers", 1, "concurrent sessions (one segment at a time each)")
		entry     = fs.String("entry", "csv", "entry: csv (LOAD DATA) | insert (multi-row INSERT)")
		batchRows = fs.Int("batch-rows", 1000, "insert entry: rows per INSERT statement")
		multiRow  = fs.Int("multi-row", 0, "insert entry: group rows sharing a column set into multi-row VALUES (0=off)")
		bulk      = fs.Bool("bulk", false, "enable SST bulk load mode (tdsql_bulk_load family) for this session")
		allowSK   = fs.Bool("allow-sk", false, "with --bulk: allow tables with secondary keys")
		unsorted  = fs.Bool("allow-unsorted", false, "with --bulk: input rows are unordered")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" || *table == "" || *dir == "" {
		return fmt.Errorf("--dsn, --table and --segments-dir are required")
	}

	files, err := filepath.Glob(filepath.Join(*dir, "*.csv"))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no segment csv files in %s", *dir)
	}
	sort.Strings(files)

	// Progress file: segment state survives crashes; finished segments are
	// skipped on re-run.
	progPath := filepath.Join(*dir, ".load-progress.json")
	prog := loadProgress(progPath)

	var doneRows, doneSegs int64
	start := time.Now()
	queue := make(chan string, len(files))
	for _, f := range files {
		if prog[filepath.Base(f)] != "done" {
			queue <- f
		} else {
			atomic.AddInt64(&doneSegs, 1)
		}
	}
	close(queue)

	var wg sync.WaitGroup
	errCh := make(chan error, *workers)
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range queue {
				if err := loadSegment(*dsn, *table, f, *entry, *batchRows, *bulk, *allowSK, *unsorted, *multiRow, &doneRows); err != nil {
					errCh <- fmt.Errorf("%s: %w", f, err)
					return
				}
				markSegment(progPath, f)
				n := atomic.AddInt64(&doneSegs, 1)
				el := time.Since(start).Seconds()
				fmt.Printf("[progress] segments %d/%d done, %d rows, %.0f rows/s, elapsed %.0fs\n",
					n, len(files), atomic.LoadInt64(&doneRows), float64(atomic.LoadInt64(&doneRows))/el, el)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	fmt.Printf("load finished: %d segments, %d rows in %.0fs\n", len(files), doneRows, time.Since(start).Seconds())
	return nil
}

// loadSegment loads one segment file in a single session.
func loadSegment(dsn, table, file, entry string, batchRows int, bulk, allowSK, unsorted bool, multiRow int, doneRows *int64) error {
	if entry == "insert" {
		dsn = ensureDSNParam(dsn, "multiStatements", "true")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if bulk {
		for _, s := range []string{
			"SET SESSION tdsql_bulk_load_allow_sk = " + onOff(allowSK),
			"SET SESSION tdsql_bulk_load_allow_unsorted = " + onOff(unsorted),
			"SET SESSION tdsql_bulk_load = ON",
		} {
			if _, err := db.Exec(s); err != nil {
				return fmt.Errorf("%s: %w", s, err)
			}
		}
	}

	cols, err := readColumns(file + ".columns")
	if err != nil {
		return err
	}

	switch entry {
	case "csv":
		mysql.RegisterLocalFile(file)
		defer mysql.DeregisterLocalFile(file)
		stmt := fmt.Sprintf(
			"LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (%s)",
			file, table, strings.Join(cols, ","))
		res, err := db.Exec(stmt)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		atomic.AddInt64(doneRows, n)
		return nil
	case "insert":
		return insertFile(db, table, file, cols, batchRows, doneRows, multiRow)
	default:
		return fmt.Errorf("unknown --entry %q (csv | insert)", entry)
	}
}

func onOff(b bool) string {
	if b {
		return "ON"
	}
	return "OFF"
}

// readColumns reads the sidecar column list written by gen.
func readColumns(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("columns sidecar %s: %w (generate data with gen, it writes <file>.columns)", path, err)
	}
	defer f.Close()
	var cols []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if s := strings.TrimSpace(sc.Text()); s != "" {
			cols = append(cols, s)
		}
	}
	return cols, sc.Err()
}

// insertFile replays a CSV segment as multi-row INSERT statements. Every
// field is emitted as a quoted SQL string literal; the server coerces types.
// insertFile replays a CSV segment as per-row INSERT statements, each
// carrying only the row's filled columns (sparse writes are the target
// shape: rows of a wide table fill a small subset of the column pool).
// batchRows statements are joined with ';' into one multi-statement Exec
// (requires multiStatements in the DSN; loadSegment ensures it).
func insertFile(db *sql.DB, table, file string, cols []string, batchRows int, doneRows *int64, multiRow int) error {
	if multiRow > 0 {
		return insertFileMultiRow(db, table, file, cols, batchRows, doneRows, multiRow)
	}
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReaderSize(f, 4*1024*1024))
	r.ReuseRecord = true

	var sb strings.Builder
	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		// Wrap the batch in an explicit transaction: one txn per batch
		// instead of one autocommit txn per row. sb ends with ";".
		if _, err := db.Exec("BEGIN;" + sb.String() + "COMMIT"); err != nil {
			return err
		}
		atomic.AddInt64(doneRows, int64(n))
		sb.Reset()
		n = 0
		return nil
	}
	// Scratch buffers for one row's column list / value list.
	var rowCols, rowVals []string
	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		rowCols = rowCols[:0]
		rowVals = rowVals[:0]
		for i, v := range rec {
			if v == `\N` {
				continue
			}
			rowCols = append(rowCols, cols[i])
			rowVals = append(rowVals, "'"+escapeSQL(v)+"'")
		}
		sb.WriteString("INSERT INTO ")
		sb.WriteString(table)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(rowCols, ","))
		sb.WriteString(") VALUES (")
		sb.WriteString(strings.Join(rowVals, ","))
		sb.WriteString(");")
		n++
		if n >= batchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// insertFileMultiRow groups rows sharing the same non-null column set into
// multi-row VALUES statements: one INSERT covers many rows, cutting the
// per-statement parse/execute cost that dominates SQL-layer throughput.
// Rows with a unique column set fall back to single-row statements.
func insertFileMultiRow(db *sql.DB, table, file string, cols []string, batchRows int, doneRows *int64, multiRow int) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReaderSize(f, 4*1024*1024))
	r.ReuseRecord = false // rows are buffered across groups

	var sb strings.Builder
	n := 0 // rows in current txn batch
	// pending group: column signature -> statement pieces
	type group struct {
		cols string
		name []string
		vals []string
		rows int
	}
	groups := map[string]*group{}
	var order []*group // deterministic flush order

	flushGroup := func(g *group) {
		if g.rows == 0 {
			return
		}
		sb.WriteString("INSERT INTO ")
		sb.WriteString(table)
		sb.WriteString(" (")
		sb.WriteString(g.cols)
		sb.WriteString(") VALUES ")
		sb.WriteString(strings.Join(g.vals, ","))
		sb.WriteString(";")
		n += g.rows
		g.vals = g.vals[:0]
		g.rows = 0
	}
	flush := func() error {
		if n == 0 {
			return nil
		}
		for _, g := range order {
			flushGroup(g)
		}
		if _, err := db.Exec("BEGIN;" + sb.String() + "COMMIT"); err != nil {
			return err
		}
		atomic.AddInt64(doneRows, int64(n))
		sb.Reset()
		n = 0
		return nil
	}

	for {
		rec, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		var names []string
		var vals []string
		for i, v := range rec {
			if v == `\N` {
				continue
			}
			names = append(names, cols[i])
			vals = append(vals, "'"+escapeSQL(v)+"'")
		}
		sig := strings.Join(names, ",")
		g := groups[sig]
		if g == nil {
			g = &group{cols: sig, name: names}
			groups[sig] = g
			order = append(order, g)
		}
		g.vals = append(g.vals, "("+strings.Join(vals, ",")+")")
		g.rows++
		if g.rows >= multiRow {
			flushGroup(g)
		}
		if n >= batchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	// flush remaining rows
	for _, g := range order {
		flushGroup(g)
	}
	return flush()
}
func escapeSQL(s string) string {
	if !strings.ContainsAny(s, "'\\") {
		return s
	}
	r := strings.NewReplacer("\\", "\\\\", "'", "\\'")
	return r.Replace(s)
}

// ensureDSNParam appends ?param=value to the DSN unless already present.
func ensureDSNParam(dsn, param, value string) string {
	if strings.Contains(dsn, param+"=") {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + param + "=" + value
}

// loadProgress reads the per-segment progress file.
func loadProgress(path string) map[string]string {
	m := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Fields(sc.Text())
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

// markSegment appends a "done" record for the segment (append-only file).
func markSegment(path, file string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s done\n", filepath.Base(file))
}
