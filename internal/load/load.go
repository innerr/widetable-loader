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
				if err := loadSegment(*dsn, *table, f, *entry, *batchRows, *bulk, *allowSK, *unsorted, &doneRows); err != nil {
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
func loadSegment(dsn, table, file, entry string, batchRows int, bulk, allowSK, unsorted bool, doneRows *int64) error {
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
		return insertFile(db, table, file, cols, batchRows, doneRows)
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
func insertFile(db *sql.DB, table, file string, cols []string, batchRows int, doneRows *int64) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	head := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(cols, ","))
	r := csv.NewReader(bufio.NewReaderSize(f, 4*1024*1024))
	r.ReuseRecord = true

	var sb strings.Builder
	sb.WriteString(head)
	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		if _, err := db.Exec(sb.String()); err != nil {
			return err
		}
		atomic.AddInt64(doneRows, int64(n))
		sb.Reset()
		sb.WriteString(head)
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
		if n > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		for i, v := range rec {
			if i > 0 {
				sb.WriteByte(',')
			}
			if v == `\N` {
				sb.WriteString("NULL")
				continue
			}
			sb.WriteByte('\'')
			sb.WriteString(escapeSQL(v))
			sb.WriteByte('\'')
		}
		sb.WriteByte(')')
		n++
		if n >= batchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// escapeSQL escapes a value for use inside a single-quoted SQL literal.
func escapeSQL(s string) string {
	if !strings.ContainsAny(s, "'\\") {
		return s
	}
	r := strings.NewReplacer("\\", "\\\\", "'", "\\'")
	return r.Replace(s)
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
