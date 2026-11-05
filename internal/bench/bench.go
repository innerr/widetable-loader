// Package bench implements the "bench" subcommand: online read/write
// workload against loaded data:
//
//   - writes: transactions updating 2-3 keys with split-column statements,
//     targeting gap keys in new mode and occupied keys in existing mode;
//   - reads: uniform point queries on occupied primary key ranges;
//   - a configurable read/write mix, fixed duration, per-thread stats.
//
// Key ranges are derived from the same parameters as gen (segments,
// total-rows, gap-factor, scatter), so bench and gen always agree on
// which keys are occupied and which are gaps.
package bench

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/genkey"
	"github.com/innerr/widetable-loader/internal/sqlutil"
)

// Params holds every knob of a bench run.
type Params struct {
	DSN       string  `json:"dsn"`
	Table     string  `json:"table"`
	Threads   int     `json:"threads"`
	DurationS int     `json:"duration_s"`
	WarmupS   int     `json:"warmup_s"`   // unrecorded warmup before the measured window
	ReadRatio float64 `json:"read_ratio"` // 0..1; rest is writes
	WriteMode string  `json:"write_mode"` // new: gap keys; existing: occupied keys

	// Key range, same semantics as gen:
	Segments  uint64 `json:"segments"`
	TotalRows uint64 `json:"total_rows"`
	GapFactor uint64 `json:"gap_factor"`
	Scatter   string `json:"scatter"`

	// Row shape, same semantics as gen:
	FixedCols int `json:"fixed_cols"` // f1..fN declared before the c columns
	Fill      int `json:"fill"`       // columns written per split row (<= file cols)
	// 宽表实际列清单（逗号分隔，不含 id）与其中的数字列。为空时用 gen 生成的
	// c000001.. 列名，仅适用于声明列形态的表。
	Columns  string `json:"columns"`
	Numeric  string `json:"numeric_columns"`
	RowWidth int    `json:"row_width"`
	Seed     uint64 `json:"seed"`
}

// Report is one snapshot of a bench run.
type Report struct {
	ElapsedS   float64 `json:"elapsed_s"`
	Reads      int64   `json:"reads"`
	Writes     int64   `json:"writes"`
	ReadRPS    float64 `json:"read_rps"`
	WriteRPS   float64 `json:"write_rps"`
	Errors     int64   `json:"errors"`
	ReadBytes  int64   `json:"read_bytes"`
	WriteRows  int64   `json:"write_rows"`
	WriteStmts int64   `json:"write_statements"`
	ReadMBPS   float64 `json:"read_mb_per_sec"`
	ReadP50MS  float64 `json:"read_p50_ms"`
	ReadP95MS  float64 `json:"read_p95_ms"`
	ReadP99MS  float64 `json:"read_p99_ms"`
	WriteP50MS float64 `json:"write_p50_ms"`
	WriteP95MS float64 `json:"write_p95_ms"`
	WriteP99MS float64 `json:"write_p99_ms"`
}

// columnList 返回要写的列名与数字列集合。
func (this Params) columnList(cfg gen.Config) ([]string, map[string]bool) {
	numeric := map[string]bool{}
	for _, name := range strings.Split(this.Numeric, ",") {
		if name = strings.TrimSpace(name); name != "" {
			numeric[name] = true
		}
	}
	if this.Columns == "" {
		cols := cfg.ColumnNames()[1:]
		for index := range cols {
			if index < cfg.FixedCols && index != 2 && index != 3 {
				numeric[cols[index]] = true
			}
		}
		return cols, numeric
	}
	var cols []string
	for _, name := range strings.Split(this.Columns, ",") {
		if name = strings.TrimSpace(name); name != "" {
			cols = append(cols, name)
		}
	}
	return cols, numeric
}

func (this Params) Validate() error {
	writeMode := this.WriteMode
	if writeMode == "" {
		writeMode = "new"
	}
	if writeMode != "new" && writeMode != "existing" {
		return fmt.Errorf("unknown write-mode %q (new | existing)", this.WriteMode)
	}
	if this.DSN == "" || this.Table == "" || this.Threads < 1 || this.DurationS < 1 ||
		this.WarmupS < 0 || this.WarmupS >= this.DurationS ||
		math.IsNaN(this.ReadRatio) || this.ReadRatio < 0 || this.ReadRatio > 1 || this.Fill < 1 {
		return fmt.Errorf("dsn, table, positive threads/duration/fill, warmup below duration and read-ratio in [0,1] are required")
	}
	cfg := gen.Config{TotalRows: this.TotalRows, Segments: this.Segments, Scatter: this.Scatter,
		GapFactor: this.GapFactor, Fields: this.Fill, Fill: this.Fill, FixedCols: this.FixedCols, RowWidth: this.RowWidth}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if writeMode == "existing" {
		declared, _ := this.columnList(cfg)
		return validateExistingColumns(declared)
	}
	if this.ReadRatio < 1 {
		if this.GapFactor <= 1 || this.TotalRows > uint64(1<<63-1)/this.GapFactor {
			return fmt.Errorf("writes require gap-factor > 1 within signed BIGINT range")
		}
		if this.Scatter == "hash32" && this.TotalRows*this.GapFactor > 1<<32 {
			return fmt.Errorf("hash32 write domain exceeds 2^32")
		}
	}
	return nil
}

// Run parses flags and runs the workload until done.
func Run(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	var p Params
	fs.StringVar(&p.DSN, "dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:port)/db")
	fs.StringVar(&p.Table, "table", "", "target table")
	fs.IntVar(&p.Threads, "threads", 16, "concurrent client threads")
	fs.IntVar(&p.DurationS, "duration", 600, "run duration in seconds")
	fs.IntVar(&p.WarmupS, "warmup", 0, "unrecorded warmup seconds before the measured window")
	fs.Float64Var(&p.ReadRatio, "read-ratio", 0.5, "read fraction 0..1 (rest is writes)")
	fs.StringVar(&p.WriteMode, "write-mode", "new", "write target: new gap keys | existing occupied keys")
	fs.StringVar(&p.Columns, "columns", "", "comma-separated declared columns to write, excluding id")
	fs.Uint64Var(&p.Segments, "segments", 0, "segment count the data was generated with")
	fs.Uint64Var(&p.TotalRows, "total-rows", 0, "total rows the data was generated with")
	fs.Uint64Var(&p.GapFactor, "gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor")
	fs.StringVar(&p.Scatter, "scatter", "hash32", "key layout used by gen: hash32 (customer scatter, default) | blocked | none | mul-hash")
	fs.IntVar(&p.FixedCols, "fixed-cols", 6, "fixed f-column count (same as gen)")
	fs.IntVar(&p.Fill, "fill", 30, "columns written per split row (must be <= gen's file-cols)")
	fs.IntVar(&p.RowWidth, "row-width", 1024, "target row width (value sizing)")
	fs.Uint64Var(&p.Seed, "seed", 1, "random seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	gapFactorSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "gap-factor" {
			gapFactorSet = true
		}
	})
	if p.WriteMode == "existing" && gapFactorSet {
		return fmt.Errorf("-gap-factor has no effect with -write-mode existing")
	}
	if p.DSN == "" || p.Table == "" {
		return fmt.Errorf("--dsn and --table are required")
	}
	if p.Segments == 0 || p.TotalRows == 0 {
		return fmt.Errorf("--segments and --total-rows are required (match the gen run)")
	}
	if p.GapFactor == 0 {
		p.GapFactor = 10
	}
	if p.Scatter == "" {
		p.Scatter = "blocked"
	}
	return RunWithReport(context.Background(), p, func(r Report) {
		fmt.Printf("[bench] %.0fs reads %d (%.0f/s) %.1f MB/s p50 %.1fms p99 %.1fms, writes %d (%.0f/s) errs %d\n",
			r.ElapsedS, r.Reads, r.ReadRPS, r.ReadMBPS, r.ReadP50MS, r.ReadP99MS,
			r.Writes, r.WriteRPS, r.Errors)
	})
}

// RunWithReport runs the workload, calling onReport every 5 seconds and at
// the end. ctx cancellation stops the run early.
func RunWithReport(ctx context.Context, p Params, onReport func(Report)) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if onReport == nil {
		onReport = func(Report) {}
	}
	parent := ctx
	cfg := gen.Config{
		TotalRows:   p.TotalRows,
		Segments:    p.Segments,
		Scatter:     p.Scatter,
		GapFactor:   p.GapFactor,
		FixedCols:   p.FixedCols,
		Fill:        p.Fill,
		RowWidth:    p.RowWidth,
		Fields:      p.Fill, // only the columns we write matter
		FileCols:    p.Fill,
		IndexFields: 0,
		Seed:        p.Seed,
	}
	// Occupied/gap key ranges: gen uses global sequences [seg*rps,
	// seg*rps+rps) fed through the segment's key layout; gap sequences for
	// writes are [totalRows, totalRows*gapFactor), which blocked also
	// leaves empty (the stride gaps) and scattered layouts leave unused
	// by construction.
	rps := cfg.RowsPerSeg() // rows actually generated per segment
	gapLo := cfg.TotalRows
	gapHi := cfg.TotalRows * cfg.GapFactor
	if gapHi <= gapLo {
		gapHi = gapLo + 1
	}
	var layouts []genkey.Layout
	for seg := uint64(0); seg < cfg.Segments; seg++ {
		l, err := cfg.Layout(seg)
		if err != nil {
			return err
		}
		layouts = append(layouts, l)
	}
	var reads, writes, errs, readBytes, writeRows, writeStmts int64
	// 延迟按样本全量收集：中间进度不复位，分位只在最终报告里算一次。
	// 600 秒 × 数千次每秒约几百万个样本，几十 MB，可接受。
	var latencies latencySamples

	start := time.Now()
	measureStart := start.Add(time.Duration(p.WarmupS) * time.Second)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reporter goroutine.
	stopRep := make(chan struct{})
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopRep:
				return
			case <-t.C:
				report(&reads, &writes, &errs, &readBytes, &writeRows, &writeStmts, measureStart,
					&latencies, false, onReport)
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, p.Threads)
	for i := 0; i < p.Threads; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			if err := runThread(ctx, p, cfg, rps, gapLo, gapHi, layouts, rng, measureStart,
				&reads, &writes, &errs, &readBytes, &writeRows, &writeStmts, &latencies); err != nil && ctx.Err() == nil {
				errCh <- err
				cancel()
			}
		}(int64(p.Seed) + int64(i))
	}

	// Wait until duration or all threads died.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	deadline := time.After(time.Duration(p.DurationS) * time.Second)
	select {
	case <-done:
	case <-deadline:
		cancel()
		<-done
	case <-ctx.Done():
		cancel()
		<-done
	}
	close(stopRep)
	<-repDone
	report(&reads, &writes, &errs, &readBytes, &writeRows, &writeStmts, measureStart,
		&latencies, true, onReport)
	select {
	case err := <-errCh:
		return err
	default:
	}
	return parent.Err()
}

// latencySamples collects read and write latencies for one run.
type latencySamples struct {
	mu     sync.Mutex
	reads  []float64
	writes []float64
}

func (this *latencySamples) addRead(ms float64) {
	this.mu.Lock()
	this.reads = append(this.reads, ms)
	this.mu.Unlock()
}

func (this *latencySamples) addWrite(ms float64) {
	this.mu.Lock()
	this.writes = append(this.writes, ms)
	this.mu.Unlock()
}

func report(reads, writes, errs, readBytes, writeRows, writeStmts *int64, measureStart time.Time,
	latencies *latencySamples, final bool, onReport func(Report)) {
	el := time.Since(measureStart).Seconds()
	if el < 0 {
		el = 0
	}
	r := Report{
		ElapsedS:   el,
		Reads:      atomic.LoadInt64(reads),
		Writes:     atomic.LoadInt64(writes),
		Errors:     atomic.LoadInt64(errs),
		ReadBytes:  atomic.LoadInt64(readBytes),
		WriteRows:  atomic.LoadInt64(writeRows),
		WriteStmts: atomic.LoadInt64(writeStmts),
	}
	if el > 0 {
		r.ReadRPS = float64(r.Reads) / el
		r.WriteRPS = float64(r.Writes) / el
		r.ReadMBPS = float64(r.ReadBytes) / el / 1e6
	}
	if final {
		latencies.mu.Lock()
		readLats := append([]float64(nil), latencies.reads...)
		writeLats := append([]float64(nil), latencies.writes...)
		latencies.mu.Unlock()
		r.ReadP50MS, r.ReadP95MS, r.ReadP99MS = percentiles(readLats)
		r.WriteP50MS, r.WriteP95MS, r.WriteP99MS = percentiles(writeLats)
	}
	onReport(r)
}

func percentiles(lats []float64) (p50, p95, p99 float64) {
	if len(lats) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(lats)
	return lats[len(lats)/2], lats[len(lats)*95/100], lats[len(lats)*99/100]
}

// runThread loops until ctx is done, mixing point reads and split writes.
func runThread(ctx context.Context, p Params, cfg gen.Config, rps, gapLo, gapHi uint64,
	layouts []genkey.Layout, rng *rand.Rand, measureStart time.Time,
	reads, writes, errs, readBytes, writeRows, writeStmts *int64, latencies *latencySamples) error {
	db, err := sql.Open("mysql", p.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		return workloadError(ctx, errs, "connect", err)
	}

	declared, numeric := p.columnList(cfg) // 不含 id
	for ctx.Err() == nil {
		measured := !time.Now().Before(measureStart)
		if rng.Float64() < p.ReadRatio {
			t0 := time.Now()
			size, err := pointRead(ctx, db, p.Table, rps, layouts, rng, cfg, errs)
			if err != nil {
				return err
			}
			if measured {
				atomic.AddInt64(reads, 1)
				atomic.AddInt64(readBytes, int64(size))
				latencies.addRead(float64(time.Since(t0).Microseconds()) / 1000)
			}
		} else {
			t0 := time.Now()
			var rows, statements int
			var err error
			if p.WriteMode == "existing" {
				rows, statements, err = existingWrite(ctx, db, p.Table, declared, numeric, rps,
					layouts, rng, cfg, errs)
			} else {
				rows, statements, err = splitWrite(ctx, db, p.Table, declared, numeric, gapLo, gapHi,
					layouts, rng, cfg, errs)
			}
			if err != nil {
				return err
			}
			if measured {
				atomic.AddInt64(writes, 1)
				atomic.AddInt64(writeRows, int64(rows))
				atomic.AddInt64(writeStmts, int64(statements))
			}
			if measured {
				latencies.addWrite(float64(time.Since(t0).Microseconds()) / 1000)
			}
		}
	}
	return nil
}

// pointRead: uniform pick on occupied ranges, whole-row SELECT by pk.
// It returns the bytes of the decoded row so the caller can report read
// bandwidth; the row must be fully received before the read counts.
func pointRead(ctx context.Context, db *sql.DB, table string, rps uint64, layouts []genkey.Layout, rng *rand.Rand, cfg gen.Config, errs *int64) (int, error) {
	seg := rng.Uint64() % cfg.Segments
	id := layouts[seg].ID(seg*rps + rng.Uint64()%rps)
	// 宽表要求 SELECT * 带字面量 LIMIT，否则报 "does not support SELECT *"。
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE id = %d LIMIT 1", sqlutil.QuoteIdentifier(table), id))
	if err != nil {
		return 0, workloadError(ctx, errs, "point read", err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, workloadError(ctx, errs, "point read", err)
	}
	raw := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(columns))
	for i := range raw {
		dest[i] = &raw[i]
	}
	size, found := 0, 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return 0, workloadError(ctx, errs, "point read", err)
		}
		for _, value := range raw {
			size += len(value)
		}
		found++
	}
	if err := rows.Err(); err != nil {
		return 0, workloadError(ctx, errs, "point read", err)
	}
	if found == 0 {
		return 0, workloadError(ctx, errs, "point read", fmt.Errorf("generated key %d is missing", id))
	}
	return size, nil
}

const (
	largeJSONColumn1    = "xx_long_1_raw"
	largeJSONColumn2    = "xx_long_2_raw"
	largeJSONValueBytes = 51200
)

type writeStatement struct {
	query   string
	args    []any
	columns []string
}

func validateExistingColumns(declared []string) error {
	found := make(map[string]bool, 2)
	for _, name := range declared {
		if name == largeJSONColumn1 || name == largeJSONColumn2 {
			found[name] = true
		}
	}
	if len(found) != 2 {
		return fmt.Errorf("-write-mode existing requires -columns to include %s and %s; the case must contain both large JSON columns",
			largeJSONColumn1, largeJSONColumn2)
	}
	if len(declared) < 3 {
		return fmt.Errorf("-write-mode existing requires at least one small column besides %s and %s",
			largeJSONColumn1, largeJSONColumn2)
	}
	return nil
}

// buildWriteStatements builds 2-3 non-overlapping column subsets for one key.
// Existing-key writes put both large JSON columns in the first subset.
func buildWriteStatements(table string, id uint64, declared []string, numeric map[string]bool,
	rng *rand.Rand, cfg gen.Config, writeMode string) ([]writeStatement, error) {
	keyword := "INSERT"
	existing := writeMode == "existing"
	if existing {
		keyword = "UPSERT"
		if err := validateExistingColumns(declared); err != nil {
			return nil, err
		}
	} else if writeMode != "new" {
		return nil, fmt.Errorf("unknown write-mode %q (new | existing)", writeMode)
	}

	perRow := 2 + rng.Intn(2)
	perm := rng.Perm(len(declared))
	if existing {
		forced := make([]int, 0, 2)
		for _, name := range []string{largeJSONColumn1, largeJSONColumn2} {
			for _, index := range perm {
				if declared[index] == name {
					forced = append(forced, index)
					break
				}
			}
		}
		remaining := make([]int, 0, len(perm)-2)
		for _, index := range perm {
			if declared[index] != largeJSONColumn1 && declared[index] != largeJSONColumn2 {
				remaining = append(remaining, index)
			}
		}
		perm = append(forced, remaining...)
	}

	perStatement := (len(perm) + perRow - 1) / perRow
	statements := make([]writeStatement, 0, perRow)
	for k := 0; k < perRow; k++ {
		lo := k * perStatement
		if lo >= len(perm) {
			break
		}
		hi := min(lo+perStatement, len(perm))
		subset := perm[lo:hi]
		colNames := make([]string, 0, len(subset)+1)
		colNames = append(colNames, "id")
		placeholders := make([]string, 0, len(subset)+1)
		placeholders = append(placeholders, "?")
		statement := writeStatement{
			args:    make([]any, 0, len(subset)+1),
			columns: make([]string, 0, len(subset)),
		}
		statement.args = append(statement.args, id)
		for _, ci := range subset {
			name := declared[ci]
			colNames = append(colNames, name)
			placeholders = append(placeholders, "?")
			statement.columns = append(statement.columns, name)
			statement.args = append(statement.args, benchColumn(name, numeric[name], rng, cfg, len(subset)))
		}
		statement.query = fmt.Sprintf("%s INTO %s (%s) VALUES (%s)", keyword,
			sqlutil.QuoteIdentifier(table), strings.Join(colNames, ","), strings.Join(placeholders, ","))
		statements = append(statements, statement)
	}
	return statements, nil
}

// splitWrite: one transaction writing 2-3 new rows into gap keys. Each row is
// written by 2-3 INSERT statements that split its column subset, so a row costs
// several statements but only one row in the table.
func splitWrite(ctx context.Context, db *sql.DB, table string, declared []string, numeric map[string]bool,
	gapLo, gapHi uint64, layouts []genkey.Layout, rng *rand.Rand, cfg gen.Config,
	errs *int64) (rows int, statements int, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, workloadError(ctx, errs, "begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	rowsPerTx := 2 + rng.Intn(2)
	for r := 0; r < rowsPerTx; r++ {
		seg := rng.Uint64() % cfg.Segments
		seq := gapLo + rng.Uint64()%(gapHi-gapLo)
		if cfg.Scatter == "blocked" {
			seq = seg*cfg.RowsPerSeg() + cfg.RowsPerSeg() + rng.Uint64()%(cfg.Stride()-cfg.RowsPerSeg())
		}
		id := layouts[seg].ID(seq)
		built, err := buildWriteStatements(table, id, declared, numeric, rng, cfg, "new")
		if err != nil {
			return rows, statements, err
		}
		for _, statement := range built {
			// 宽表不支持 ON DUPLICATE KEY UPDATE；撞主键的 INSERT 本身就是
			// 按列合并的 upsert，正是客户 partial upsert 的语义。
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return rows, statements, workloadError(ctx, errs, "split upsert", err)
			}
			statements++
		}
		if len(built) > 0 {
			rows++
		}
	}
	if err := tx.Commit(); err != nil {
		return rows, statements, workloadError(ctx, errs, "commit", err)
	}
	return rows, statements, nil
}

// existingWrite updates 2-3 occupied keys per transaction. Each key receives
// 2-3 UPSERT statements, including one statement with both large JSON columns.
func existingWrite(ctx context.Context, db *sql.DB, table string, declared []string, numeric map[string]bool,
	rps uint64, layouts []genkey.Layout, rng *rand.Rand, cfg gen.Config,
	errs *int64) (rows int, statements int, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, workloadError(ctx, errs, "begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	rowsPerTx := 2 + rng.Intn(2)
	for r := 0; r < rowsPerTx; r++ {
		seg := rng.Uint64() % cfg.Segments
		seq := seg*rps + rng.Uint64()%rps
		id := layouts[seg].ID(seq)
		built, err := buildWriteStatements(table, id, declared, numeric, rng, cfg, "existing")
		if err != nil {
			return rows, statements, err
		}
		for _, statement := range built {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return rows, statements, workloadError(ctx, errs, "existing upsert", err)
			}
			statements++
		}
		rows++
	}
	if err := tx.Commit(); err != nil {
		return rows, statements, workloadError(ctx, errs, "commit", err)
	}
	return rows, statements, nil
}

func workloadError(ctx context.Context, errs *int64, action string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	atomic.AddInt64(errs, 1)
	return fmt.Errorf("%s: %w", action, err)
}

// benchColumn builds one column value. The row width is carried by the two
// large JSON columns, so every other column gets a short value that fits its
// declared width: a numeric column gets an integer, a named customer column
// gets a short string, and a dynamic value/raw pair gets the shape gen writes.
// Spreading RowWidth over all columns would overflow VARCHAR and bigint
// columns on the customer table (seen with --row-width 120000).
func benchColumn(name string, numeric bool, rng *rand.Rand, cfg gen.Config, count int) any {
	if name == largeJSONColumn1 || name == largeJSONColumn2 {
		return benchLargeJSONValue(rng)
	}
	if numeric || name == "pgId" || name == "parentId" || name == "dwId" ||
		name == "createTime" || name == "updateTime" || name == "mi_isDeleted" ||
		strings.HasSuffix(name, "_val") && strings.HasPrefix(name, "mi_") {
		return int64(1700000000) + rng.Int63n(1000000)
	}
	if strings.HasSuffix(name, "_raw") {
		return fmt.Sprintf(`{"v":%d,"p":"%s"}`, rng.Intn(1000000), benchRandomString(rng, 24))
	}
	if strings.HasSuffix(name, "_val") {
		switch {
		case strings.HasPrefix(name, "md_"):
			return fmt.Sprintf("%.2f", rng.Float64()*1000)
		case strings.HasPrefix(name, "mb_"):
			return "true"
		default:
			return benchRandomString(rng, 18)
		}
	}
	// Named customer columns: short values that fit varchar(16..128).
	if strings.HasPrefix(name, "dataType") || strings.HasPrefix(name, "parentDataType") {
		return benchRandomString(rng, 4)
	}
	if strings.HasPrefix(name, "ms_") {
		return benchRandomString(rng, 12)
	}
	return benchValue(rng, 32, count)
}

func benchLargeJSONValue(rng *rand.Rand) string {
	head := fmt.Sprintf(`{"v":%d,"p":"`, rng.Intn(1000000))
	tail := `"}`
	return head + benchRandomString(rng, largeJSONValueBytes-len(head)-len(tail)) + tail
}

func benchColumnValue(rng *rand.Rand, idx int, cfg gen.Config, count int) any {
	if idx < cfg.FixedCols {
		switch idx {
		case 0:
			return int64(1700000000) + rng.Int63n(1000000)
		case 1:
			return 0
		default:
			return "fixed"
		}
	}
	switch (idx - cfg.FixedCols) % 5 {
	case 0:
		return rng.Intn(1000000)
	case 2:
		return rng.Float64() * 1000
	case 3:
		return "true"
	case 4:
		return fmt.Sprintf(`{"value":%d}`, rng.Intn(1000000))
	default:
		return benchValue(rng, cfg.RowWidth, count)
	}
}

// benchValue returns a plausible column value sized for the target row
// width: strings dominate, ints/doubles for variety like gen does.
func benchValue(rng *rand.Rand, rowWidth, colsInRow int) interface{} {
	n := rowWidth / (colsInRow * 2) // utf8mb3: ~1 byte per char
	if n < 4 {
		n = 4
	}
	return benchRandomString(rng, n)
}

func benchRandomString(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
