// Package bench implements the "bench" subcommand: online read/write
// workload against loaded data:
//
//   - writes: transactions of 2-3 split-row UPSERTs, primary keys drawn
//     from the gap ranges the generator left between segments;
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
	"errors"
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
	ReadRatio float64 `json:"read_ratio"` // 0..1; rest is writes

	// Key range, same semantics as gen:
	Segments  uint64 `json:"segments"`
	TotalRows uint64 `json:"total_rows"`
	GapFactor uint64 `json:"gap_factor"`
	Scatter   string `json:"scatter"`

	// Row shape, same semantics as gen:
	FixedCols int    `json:"fixed_cols"` // f1..fN declared before the c columns
	Fill      int    `json:"fill"`       // columns written per split row (<= file cols)
	RowWidth  int    `json:"row_width"`
	Seed      uint64 `json:"seed"`
}

// Report is one snapshot of a bench run.
type Report struct {
	ElapsedS   float64 `json:"elapsed_s"`
	Reads      int64   `json:"reads"`
	Writes     int64   `json:"writes"`
	ReadRPS    float64 `json:"read_rps"`
	WriteRPS   float64 `json:"write_rps"`
	Errors     int64   `json:"errors"`
	WriteP99MS float64 `json:"write_p99_ms"`
	WriteP50MS float64 `json:"write_p50_ms"`
}

func (this Params) Validate() error {
	if this.DSN == "" || this.Table == "" || this.Threads < 1 || this.DurationS < 1 ||
		math.IsNaN(this.ReadRatio) || this.ReadRatio < 0 || this.ReadRatio > 1 || this.Fill < 1 {
		return fmt.Errorf("dsn, table, positive threads/duration/fill and read-ratio in [0,1] are required")
	}
	cfg := gen.Config{TotalRows: this.TotalRows, Segments: this.Segments, Scatter: this.Scatter,
		GapFactor: this.GapFactor, Fields: this.Fill, Fill: this.Fill, FixedCols: this.FixedCols, RowWidth: this.RowWidth}
	if err := cfg.Validate(); err != nil {
		return err
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
	fs.Float64Var(&p.ReadRatio, "read-ratio", 0.5, "read fraction 0..1 (rest is split-row UPSERT writes)")
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
		fmt.Printf("[bench] %.0fs reads %d (%.0f/s) writes %d (%.0f/s) errs %d p50 %.1fms p99 %.1fms\n",
			r.ElapsedS, r.Reads, r.ReadRPS, r.Writes, r.WriteRPS, r.Errors, r.WriteP50MS, r.WriteP99MS)
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
	var reads, writes, errs int64
	latCh := make(chan float64, 1<<16) // write latencies in ms, for percentiles

	start := time.Now()
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
				report(&reads, &writes, &errs, start, latCh, false, onReport)
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
			if err := runThread(ctx, p, cfg, rps, gapLo, gapHi, layouts, rng, &reads, &writes, &errs, latCh); err != nil && ctx.Err() == nil {
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
	report(&reads, &writes, &errs, start, latCh, true, onReport)
	select {
	case err := <-errCh:
		return err
	default:
	}
	return parent.Err()
}

func report(reads, writes, errs *int64, start time.Time, latCh chan float64, final bool, onReport func(Report)) {
	el := time.Since(start).Seconds()
	r := Report{
		ElapsedS: el,
		Reads:    atomic.LoadInt64(reads),
		Writes:   atomic.LoadInt64(writes),
		Errors:   atomic.LoadInt64(errs),
	}
	if el > 0 {
		r.ReadRPS = float64(r.Reads) / el
		r.WriteRPS = float64(r.Writes) / el
	}
	// Drain latencies for percentiles.
	var lats []float64
	for {
		select {
		case v := <-latCh:
			lats = append(lats, v)
		default:
			goto done
		}
	}
done:
	if len(lats) > 0 {
		sort.Float64s(lats)
		r.WriteP50MS = lats[len(lats)/2]
		r.WriteP99MS = lats[len(lats)*99/100]
	}
	onReport(r)
}

// runThread loops until ctx is done, mixing point reads and split writes.
func runThread(ctx context.Context, p Params, cfg gen.Config, rps, gapLo, gapHi uint64, layouts []genkey.Layout, rng *rand.Rand, reads, writes, errs *int64, latCh chan float64) error {
	db, err := sql.Open("mysql", p.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		return workloadError(ctx, errs, "connect", err)
	}

	cols := cfg.ColumnNames() // id, f1.., c000001..
	for ctx.Err() == nil {
		if rng.Float64() < p.ReadRatio {
			if err := pointRead(ctx, db, p.Table, cols, rps, layouts, rng, cfg, reads, errs); err != nil {
				return err
			}
		} else {
			t0 := time.Now()
			if err := splitWrite(ctx, db, p.Table, cols, gapLo, gapHi, layouts, rng, cfg, writes, errs); err != nil {
				return err
			}
			select {
			case latCh <- float64(time.Since(t0).Microseconds()) / 1000:
			default:
			}
		}
	}
	return nil
}

// pointRead: uniform pick on occupied ranges, SELECT by pk.
func pointRead(ctx context.Context, db *sql.DB, table string, cols []string, rps uint64, layouts []genkey.Layout, rng *rand.Rand, cfg gen.Config, reads, errs *int64) error {
	seg := rng.Uint64() % cfg.Segments
	id := layouts[seg].ID(seg*rps + rng.Uint64()%rps)
	var dummy int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT id FROM %s WHERE id = %d", sqlutil.QuoteIdentifier(table), id)).Scan(&dummy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workloadError(ctx, errs, "point read", fmt.Errorf("generated key %d is missing", id))
		}
		return workloadError(ctx, errs, "point read", err)
	}
	atomic.AddInt64(reads, 1)
	return nil
}

// splitWrite: one transaction of 2-3 split-row UPSERTs into gap keys.
// Each split row writes a distinct column subset, so the 2-3 rows together
// form one full wide row (the baseline's "split-row UPSERT" shape).
func splitWrite(ctx context.Context, db *sql.DB, table string, cols []string, gapLo, gapHi uint64, layouts []genkey.Layout, rng *rand.Rand, cfg gen.Config, writes, errs *int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return workloadError(ctx, errs, "begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	parts := 2 + int(rng.Intn(2)) // 2 or 3 split rows
	declared := cols[1:]          // without id
	// Shuffle column indices once, then split into parts.
	perm := rng.Perm(len(declared))
	perPart := (len(perm) + parts - 1) / parts

	seg := rng.Uint64() % cfg.Segments
	seq := gapLo + rng.Uint64()%(gapHi-gapLo)
	if cfg.Scatter == "blocked" {
		seq = seg*cfg.RowsPerSeg() + cfg.RowsPerSeg() + rng.Uint64()%(cfg.Stride()-cfg.RowsPerSeg())
	}
	id := layouts[seg].ID(seq)
	for k := 0; k < parts; k++ {
		lo := k * perPart
		if lo >= len(perm) {
			break
		}
		hi := lo + perPart
		if hi > len(perm) {
			hi = len(perm)
		}
		subset := perm[lo:hi]
		if len(subset) == 0 {
			continue
		}
		// gap key: any global sequence outside the occupied domain,
		// mapped through one segment's layout.
		colNames := make([]string, 0, len(subset)+1)
		colNames = append(colNames, "id")
		placeholders := make([]string, 0, len(subset)+1)
		placeholders = append(placeholders, "?")
		args := make([]interface{}, 0, len(subset)+1)
		args = append(args, id)
		updates := make([]string, 0, len(subset))
		for _, ci := range subset {
			name := declared[ci]
			colNames = append(colNames, name)
			placeholders = append(placeholders, "?")
			args = append(args, benchColumnValue(rng, ci, cfg, len(subset)))
			updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", name, name))
		}
		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			sqlutil.QuoteIdentifier(table), strings.Join(colNames, ","), strings.Join(placeholders, ","), strings.Join(updates, ","))
		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			return workloadError(ctx, errs, "split upsert", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return workloadError(ctx, errs, "commit", err)
	}
	atomic.AddInt64(writes, 1)
	return nil
}

func workloadError(ctx context.Context, errs *int64, action string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	atomic.AddInt64(errs, 1)
	return fmt.Errorf("%s: %w", action, err)
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
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	n := rowWidth / (colsInRow * 2) // utf8mb3: ~1 byte per char
	if n < 4 {
		n = 4
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
