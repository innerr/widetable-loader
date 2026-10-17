// Package load imports CSV segments with transaction-scoped progress.
package load

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/innerr/widetable-loader/internal/sqlutil"
)

type Params struct {
	DSN       string `json:"dsn"`
	Table     string `json:"table"`
	Dir       string `json:"segments_dir"`
	Workers   int    `json:"workers"`
	Entry     string `json:"entry"`
	BatchRows int    `json:"batch_rows"`
	MultiRow  int    `json:"multi_row"`
	Prepared  bool   `json:"prepared"`
	Bulk      bool   `json:"bulk"`
	AllowSK   bool   `json:"allow_sk"`
	Unsorted  bool   `json:"allow_unsorted"`
	NoResume  bool   `json:"no_resume"`
}

type Progress struct {
	SegmentsDone  int     `json:"segments_done"`
	SegmentsTotal int     `json:"segments_total"`
	Rows          int64   `json:"rows"`
	RowsPerSec    float64 `json:"rows_per_sec"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	Done          bool    `json:"done"`
}

func (this Params) Validate() error {
	if this.DSN == "" || this.Table == "" || this.Dir == "" {
		return errors.New("dsn, table and segments-dir are required")
	}
	if this.Workers < 1 || this.BatchRows < 1 || this.MultiRow < 0 {
		return errors.New("workers and batch-rows must be positive; multi-row must be nonnegative")
	}
	if this.Entry != "csv" && this.Entry != "insert" && this.Entry != "upsert" {
		return fmt.Errorf("unknown entry %q (csv | insert | upsert)", this.Entry)
	}
	if this.Prepared && this.MultiRow > 0 {
		return errors.New("prepared and multi-row cannot be combined")
	}
	if this.Entry == "csv" && (this.Prepared || this.MultiRow > 0) {
		return errors.New("csv entry does not accept prepared or multi-row")
	}
	_, err := mysql.ParseDSN(this.DSN)
	return err
}

func Run(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	var p Params
	fs.StringVar(&p.DSN, "dsn", "", "MySQL DSN")
	fs.StringVar(&p.Table, "table", "", "target table")
	fs.StringVar(&p.Dir, "segments-dir", "", "segment CSV directory")
	fs.IntVar(&p.Workers, "workers", 1, "concurrent sessions")
	fs.StringVar(&p.Entry, "entry", "upsert", "csv | insert | upsert")
	fs.IntVar(&p.BatchRows, "batch-rows", 1000, "rows per transaction")
	fs.IntVar(&p.MultiRow, "multi-row", 0, "rows per VALUES statement (0 = one row)")
	fs.BoolVar(&p.Prepared, "prepared", false, "cache prepared statements by column set")
	fs.BoolVar(&p.Bulk, "bulk", false, "enable SST bulk mode")
	fs.BoolVar(&p.AllowSK, "allow-sk", false, "bulk: allow secondary keys")
	fs.BoolVar(&p.Unsorted, "allow-unsorted", false, "bulk: unordered input")
	fs.BoolVar(&p.NoResume, "no-resume", false, "ignore previous completion records and load every file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return RunWithProgress(context.Background(), p, func(pr Progress) {
		fmt.Printf("[progress] segments %d/%d, %d rows, done=%v\n", pr.SegmentsDone, pr.SegmentsTotal, pr.Rows, pr.Done)
	})
}

type completed struct {
	Hash string `json:"sha256"`
	Rows int64  `json:"rows"`
}

type manifest struct {
	Target   string               `json:"target"`
	Files    map[string]completed `json:"files"`
	Inflight map[string]string    `json:"inflight,omitempty"`
}

func RunWithProgress(ctx context.Context, p Params, onProgress func(Progress)) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join(p.Dir, "*.csv"))
	if err != nil {
		return err
	}
	sqlSegments := false
	if len(files) == 0 {
		files, err = filepath.Glob(filepath.Join(p.Dir, "*.sql"))
		if err != nil {
			return err
		}
		sqlSegments = len(files) > 0
	}
	if len(files) == 0 {
		return fmt.Errorf("no segment csv or sql files in %s", p.Dir)
	}
	if !sqlSegments {
		if extra, _ := filepath.Glob(filepath.Join(p.Dir, "*.sql")); len(extra) > 0 {
			return fmt.Errorf("mixed csv and sql segments in %s; use one format per directory", p.Dir)
		}
	}
	sort.Strings(files)
	lock, err := os.OpenFile(filepath.Join(p.Dir, ".load.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("another load owns this directory: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	cfg, _ := mysql.ParseDSN(p.DSN)
	cfg.Passwd = ""
	target := cfg.FormatDSN() + "/" + p.Table
	path := filepath.Join(p.Dir, ".load-progress.json")
	state := manifest{Target: target, Files: map[string]completed{}}
	defer os.Remove(path + ".new")
	if !p.NoResume {
		b, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			if err := json.Unmarshal(b, &state); err != nil {
				return fmt.Errorf("invalid progress file; use --no-resume to load again: %w", err)
			}
			if state.Target != target || state.Files == nil {
				return errors.New("progress belongs to a different target; use --no-resume to load again")
			}
		}
	}
	if state.Inflight == nil {
		state.Inflight = map[string]string{}
	}
	present := map[string]bool{}
	for _, file := range files {
		present[filepath.Base(file)] = true
	}
	for file := range state.Files {
		if !present[file] {
			return fmt.Errorf("completed segment is missing: %s; verify the target before --no-resume", file)
		}
	}
	for file := range state.Inflight {
		if !present[file] {
			return fmt.Errorf("unfinished segment is missing: %s; verify the target before --no-resume", file)
		}
	}
	if len(state.Inflight) > 0 && (p.Entry != "upsert" || p.Bulk) {
		return errors.New("unfinished segments may contain committed rows; verify or clear the target before --no-resume; only non-bulk upsert can replay automatically")
	}
	type segment struct{ file, hash string }
	queue := make(chan segment, len(files))
	var rows, segs int64
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		hash, err := fileHash(file)
		if err != nil {
			return err
		}
		if !sqlSegments {
			sidecar, err := fileHash(file + ".columns")
			if err != nil {
				return err
			}
			hash += sidecar
		}
		if previous, ok := state.Inflight[filepath.Base(file)]; ok && previous != hash {
			return fmt.Errorf("unfinished file changed: %s; verify the target before --no-resume", file)
		}
		if done, ok := state.Files[filepath.Base(file)]; ok {
			if done.Hash != hash {
				return fmt.Errorf("completed file changed: %s; use --no-resume", file)
			}
			rows += done.Rows
			segs++
		} else {
			queue <- segment{file, hash}
		}
	}
	close(queue)
	start := time.Now()
	var mu sync.Mutex
	report := func(done bool) {
		if onProgress == nil {
			return
		}
		elapsed := time.Since(start).Seconds()
		onProgress(Progress{int(atomic.LoadInt64(&segs)), len(files), atomic.LoadInt64(&rows),
			float64(atomic.LoadInt64(&rows)) / elapsed, elapsed, done})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, p.Workers)
	var wg sync.WaitGroup
	for i := 0; i < p.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range queue {
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				state.Inflight[filepath.Base(item.file)] = item.hash
				err := saveManifest(path, state)
				mu.Unlock()
				if err != nil {
					errs <- fmt.Errorf("save pending segment: %w", err)
					cancel()
					return
				}
				count, err := loadSegment(ctx, p, item.file)
				if err != nil {
					errs <- fmt.Errorf("%s: %w", item.file, err)
					cancel()
					return
				}
				mu.Lock()
				state.Files[filepath.Base(item.file)] = completed{item.hash, count}
				delete(state.Inflight, filepath.Base(item.file))
				e := saveManifest(path, state)
				if e == nil {
					atomic.AddInt64(&rows, count)
					atomic.AddInt64(&segs, 1)
					report(false)
				}
				mu.Unlock()
				if e != nil {
					errs <- fmt.Errorf("save progress: %w", e)
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	report(true)
	return nil
}

func saveManifest(path string, state manifest) error {
	f, err := os.OpenFile(path+".new", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(state)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(path+".new", path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func quoteIdentifier(name string) string {
	return sqlutil.QuoteIdentifier(name)
}

func loadSegment(ctx context.Context, p Params, file string) (int64, error) {
	cfg, err := mysql.ParseDSN(p.DSN)
	if err != nil {
		return 0, err
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return 0, err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if p.Bulk {
		for _, setting := range []struct {
			name    string
			enabled bool
		}{
			{"tdsql_bulk_load_allow_sk", p.AllowSK}, {"tdsql_bulk_load_allow_unsorted", p.Unsorted}, {"tdsql_bulk_load", true},
		} {
			value := "OFF"
			if setting.enabled {
				value = "ON"
			}
			if _, err := conn.ExecContext(ctx, "SET SESSION "+setting.name+"="+value); err != nil {
				return 0, err
			}
		}
	}
	if strings.HasSuffix(file, ".sql") {
		if p.Entry == "csv" {
			return 0, errors.New("csv entry does not accept sql segments")
		}
		if p.Prepared {
			return 0, errors.New("prepared statements are meaningless for sql segments")
		}
		return insertSQLFile(ctx, conn, p, file)
	}
	b, err := os.ReadFile(file + ".columns")
	if err != nil {
		return 0, err
	}
	cols := strings.Fields(string(b))
	if len(cols) == 0 || cols[0] != "id" {
		return 0, errors.New("first sidecar column must be id")
	}
	seen := map[string]bool{}
	for i, c := range cols {
		if seen[c] {
			return 0, fmt.Errorf("duplicate column %q", c)
		}
		seen[c] = true
		cols[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
	}
	if p.Entry == "csv" {
		abs, err := filepath.Abs(file)
		if err != nil {
			return 0, err
		}
		mysql.RegisterLocalFile(abs)
		defer mysql.DeregisterLocalFile(abs)
		// CSV doubles quotes but does not interpret MySQL backslash escapes.
		// Re-encode with MySQL's escaping so literal backslashes survive.
		converted, err := mysqlCSV(file, len(cols))
		if err != nil {
			return 0, err
		}
		defer os.Remove(converted)
		mysql.RegisterLocalFile(converted)
		defer mysql.DeregisterLocalFile(converted)
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback()
		stmt := fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (%s)",
			escapeSQL(converted), quoteIdentifier(p.Table), strings.Join(cols, ","))
		res, err := tx.ExecContext(ctx, stmt)
		if err != nil {
			return 0, err
		}
		var warnings int
		if err := tx.QueryRowContext(ctx, "SHOW COUNT(*) WARNINGS").Scan(&warnings); err != nil {
			return 0, err
		}
		if warnings != 0 {
			return 0, fmt.Errorf("LOAD DATA produced %d warnings; transaction rolled back", warnings)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		return n, tx.Commit()
	}
	return insertFile(ctx, conn, p, file, cols)
}

// insertSQLFile replays pre-rendered INSERT statements, one per line, from
// a .sql segment: the client sends the statement text as-is and does no
// CSV parsing or SQL assembly. Transactions cut once the accumulated row
// count (tuples per statement, counted from the text) reaches batch-rows,
// so the generated rows-per-statement does not need to be re-declared.

func mysqlCSV(path string, columns int) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = columns
	reader.ReuseRecord = true
	out, err := os.CreateTemp("", "loader-mysql-*.csv")
	if err != nil {
		return "", err
	}
	result := out.Name()
	ok := false
	defer func() {
		out.Close()
		if !ok {
			os.Remove(result)
		}
	}()
	writer := bufio.NewWriterSize(out, 64*1024)
	replacer := strings.NewReplacer("\\", "\\\\", "\"", "\"\"", "\x00", "\\0",
		"\n", "\\n", "\r", "\\r", "\x1a", "\\Z")
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		for i, value := range record {
			if i > 0 {
				if err := writer.WriteByte(','); err != nil {
					return "", err
				}
			}
			if value == `\N` {
				_, err = writer.WriteString(value)
			} else {
				if err = writer.WriteByte('"'); err != nil {
					return "", err
				}
				if _, err = replacer.WriteString(writer, value); err != nil {
					return "", err
				}
				err = writer.WriteByte('"')
			}
			if err != nil {
				return "", err
			}
		}
		if err := writer.WriteByte('\n'); err != nil {
			return "", err
		}
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	ok = true
	return result, nil
}

func insertFile(ctx context.Context, conn *sql.Conn, p Params, file string, cols []string) (int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = len(cols)
	encoder := insertEncoder{table: p.Table, cols: cols, upsert: p.Entry == "upsert",
		names: make([]string, 0, len(cols)), args: make([]any, 0, len(cols)),
		cachedNames: make([]string, 0, len(cols))}
	prepared := map[string]*sql.Stmt{}
	defer func() {
		for _, stmt := range prepared {
			stmt.Close()
		}
	}()
	var total int64
	for {
		var records [][]string
		for len(records) < p.BatchRows {
			rec, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return total, err
			}
			if rec[0] == `\N` || rec[0] == "" {
				return total, errors.New("primary key must be filled")
			}
			records = append(records, rec)
		}
		if len(records) == 0 {
			return total, nil
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return total, err
		}
		err = func() error {
			defer tx.Rollback()
			for i := 0; i < len(records); {
				stmt, args, group := encoder.encode(records, i, p.MultiRow)
				if p.Prepared {
					cached := prepared[stmt]
					if cached == nil {
						cached, err = conn.PrepareContext(ctx, stmt)
						if err != nil {
							return err
						}
						prepared[stmt] = cached
					}
					bound := tx.StmtContext(ctx, cached)
					_, err = bound.ExecContext(ctx, args...)
					bound.Close()
				} else {
					_, err = tx.ExecContext(ctx, stmt, args...)
				}
				if err != nil {
					return err
				}
				i += group
			}
			return tx.Commit()
		}()
		if err != nil {
			return total, err
		}
		total += int64(len(records))
	}
}

func insertSQLFile(ctx context.Context, conn *sql.Conn, p Params, file string) (int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// One line is one statement; multi-row statements of 38KB rows at 50
	// rows already reach ~2MB, so allow generous growth.
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024*1024)
	var total, inTx int64
	var tx *sql.Tx
	commit := func() error {
		if tx == nil {
			return nil
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		total += inTx
		tx, inTx = nil, 0
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		stmt := strings.TrimSuffix(scanner.Text(), ";")
		if stmt == "" {
			continue
		}
		if tx == nil {
			tx, err = conn.BeginTx(ctx, nil)
			if err != nil {
				return total, err
			}
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			tx.Rollback()
			return total, err
		}
		inTx += int64(statementRows(stmt))
		if inTx >= int64(p.BatchRows) {
			if err := commit(); err != nil {
				return total, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if tx != nil {
			tx.Rollback()
		}
		return total, err
	}
	return total, commit()
}

// statementRows counts the VALUES tuples of an INSERT statement. Generated
// values never contain the "),(" byte sequence, so this count is exact for
// generator output.
func statementRows(stmt string) int {
	return strings.Count(stmt, "),(") + 1
}

type insertEncoder struct {
	table       string
	cols        []string
	upsert      bool
	names       []string
	args        []any
	cachedNames []string
	cachedRows  int
	cachedSQL   string
}

func (this *insertEncoder) encode(records [][]string, start, limit int) (string, []any, int) {
	clear(this.args)
	this.names = this.names[:0]
	this.args = this.args[:0]
	first := records[start]
	for i, value := range first {
		if value != `\N` {
			this.names = append(this.names, this.cols[i])
			this.args = append(this.args, value)
		}
	}
	rows := 1
	for rows < limit && start+rows < len(records) {
		next := records[start+rows]
		same := true
		for i, value := range next {
			if (value == `\N`) != (first[i] == `\N`) {
				same = false
				break
			}
		}
		if !same {
			break
		}
		for _, value := range next {
			if value != `\N` {
				this.args = append(this.args, value)
			}
		}
		rows++
	}
	if rows != this.cachedRows || !slices.Equal(this.names, this.cachedNames) {
		this.cachedSQL = insertSQL(this.table, this.names, rows, this.upsert)
		this.cachedNames = append(this.cachedNames[:0], this.names...)
		this.cachedRows = rows
	}
	return this.cachedSQL, this.args, rows
}

func filledRow(rec, cols []string) ([]string, []any) {
	var names []string
	var args []any
	for i, v := range rec {
		if v == `\N` {
			continue
		}
		names = append(names, cols[i])
		args = append(args, v)
	}
	return names, args
}

func insertSQL(table string, names []string, rows int, upsert bool) string {
	placeholders := "(" + strings.TrimSuffix(strings.Repeat("?,", len(names)), ",") + ")"
	values := strings.TrimSuffix(strings.Repeat(placeholders+",", rows), ",")
	stmt := "INSERT INTO " + quoteIdentifier(table) + " (" + strings.Join(names, ",") + ") VALUES " + values
	if upsert {
		updates := make([]string, len(names))
		for i, c := range names {
			updates[i] = c + "=VALUES(" + c + ")"
		}
		stmt += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
	}
	return stmt
}

func escapeSQL(s string) string {
	return strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s)
}
