// Package dist implements the "dist" subcommand: the controller entry that
// drives daemons on all cluster nodes. One command each for distributed
// generation, distributed loading, host-stats collection and stopping.
//
// Node spec: "host:daemon_port[:sql_port]" (sql_port defaults to ""; the
// DSN template may use {port} which is replaced by sql_port when set).
package dist

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/innerr/widetable-loader/internal/gen"
)

// Node is one daemon endpoint.
type Node struct {
	Name    string
	Addr    string // host:daemon_port
	Host    string // daemon host, for {host} substitution in DSN templates
	SQLPort string // optional, for {port} substitution in DSN templates
}

type httpError struct {
	code    int
	message string
}

func (this *httpError) Error() string { return this.message }

func parseNodes(s string) ([]Node, error) {
	var out []Node
	for i, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, ":")
		if len(fields) < 2 || len(fields) > 3 || fields[0] == "" {
			return nil, fmt.Errorf("node %q: want host:daemon_port[:sql_port]", part)
		}
		for _, port := range fields[1:] {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("node %q: invalid port", part)
			}
		}
		n := Node{Name: fmt.Sprintf("node-%d", i), Addr: fields[0] + ":" + fields[1], Host: fields[0]}
		if len(fields) == 3 {
			n.SQLPort = fields[2]
		}
		n.Addr = net.JoinHostPort(n.Host, fields[1])
		for _, old := range out {
			if old.Addr == n.Addr {
				return nil, fmt.Errorf("duplicate node %q", part)
			}
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("no nodes given")
	}
	return out, nil
}

func httpJSON(addr, method, path string, body interface{}, out interface{}) error {
	return httpJSONContext(context.Background(), addr, method, path, body, out)
}

func httpJSONContext(ctx context.Context, addr, method, path string, body interface{}, out interface{}) error {
	return httpJSONJob(ctx, addr, method, path, "", body, out)
}

func httpJSONJob(ctx context.Context, addr, method, path, jobID string, body interface{}, out interface{}) error {
	var rd *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if jobID != "" {
		req.Header.Set("X-Job-ID", jobID)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return &httpError{resp.StatusCode, fmt.Sprintf("%s %s: http %d: %s", method, path, resp.StatusCode, e.Error)}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type taskSet struct {
	ctx  context.Context
	stop context.CancelFunc
	mu   sync.Mutex
	ids  map[string][]string
}

func newTasks(limit time.Duration) *taskSet {
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(parent, limit)
	return &taskSet{ctx: ctx, stop: func() { cancel(); stop() }, ids: map[string][]string{}}
}

func (this *taskSet) submit(addr, kind string, body any) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	this.mu.Lock()
	this.ids[addr] = append(this.ids[addr], id)
	this.mu.Unlock()
	var response struct {
		ID string `json:"id"`
	}
	if err := httpJSONJob(this.ctx, addr, "POST", "/api/"+kind, id, body, &response); err != nil {
		return "", err
	}
	if response.ID == "" {
		return "", errors.New("daemon returned no job id")
	}
	if response.ID != id {
		this.mu.Lock()
		this.ids[addr] = append(this.ids[addr], response.ID)
		this.mu.Unlock()
	}
	return response.ID, nil
}

func (this *taskSet) wait(addr, id string) error {
	for {
		var job struct {
			Finished bool   `json:"finished"`
			Err      string `json:"error"`
		}
		if err := httpJSONContext(this.ctx, addr, "GET", "/api/job/"+id, nil, &job); err != nil {
			return err
		}
		if job.Err != "" {
			return fmt.Errorf("%s: %s", addr, job.Err)
		}
		if job.Finished {
			return nil
		}
		select {
		case <-this.ctx.Done():
			return this.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (this *taskSet) close(failed bool) error {
	defer this.stop()
	if !failed {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(this.ids))
	for addr, ids := range this.ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, id := range ids {
				var job struct {
					Finished bool `json:"finished"`
				}
				if err := httpJSONContext(ctx, addr, "GET", "/api/job/"+id, nil, &job); err != nil {
					var status *httpError
					if errors.As(err, &status) && status.code == http.StatusNotFound {
						continue
					}
					errs <- fmt.Errorf("cannot check cleanup of %s/%s: %w", addr, id, err)
					return
				}
				if job.Finished {
					continue
				}
				if err := httpJSONContext(ctx, addr, "POST", "/api/stop/"+id, nil, nil); err != nil {
					errs <- fmt.Errorf("cannot stop %s/%s: %w", addr, id, err)
					return
				}
				for !job.Finished {
					if err := httpJSONContext(ctx, addr, "GET", "/api/job/"+id, nil, &job); err != nil {
						errs <- fmt.Errorf("cannot confirm stop of %s/%s: %w", addr, id, err)
						return
					}
					if !job.Finished {
						select {
						case <-ctx.Done():
							errs <- fmt.Errorf("job still running: %s/%s", addr, id)
							return
						case <-time.After(100 * time.Millisecond):
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}

// Run dispatches subcommands: gen | load | stats | stop.
func Run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: widetable-loader dist <gen|load|bench|stats|stop> [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "gen":
		return runGen(rest)
	case "load":
		return runLoad(rest)
	case "bench":
		return runBench(rest)
	case "stats":
		return runStats(rest)
	case "stop":
		return runStop(rest)
	default:
		return fmt.Errorf("dist: unknown subcommand %q", sub)
	}
}

// ---------- gen ----------

func runGen(args []string) (err error) {
	fs := flag.NewFlagSet("dist gen", flag.ContinueOnError)
	var (
		nodesSpec   = fs.String("nodes", "", `comma list "host:daemon_port[:sql_port]"`)
		segments    = fs.Uint64("segments", 0, "total segment count across all nodes")
		totalRows   = fs.Uint64("total-rows", 0, "total rows across all segments")
		outDir      = fs.String("out-dir", "", "segment output directory (same local path on every node)")
		scatter     = fs.String("scatter", "hash32", "key layout: hash32 (customer scatter, default) | blocked | none | mul-hash")
		gapFactor   = fs.Uint64("gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor")
		seed        = fs.Uint64("seed", 1, "random seed (identical data for same seed/params)")
		fields      = fs.Int("fields", 5000, "declared column count")
		fileCols    = fs.Int("file-cols", 0, "declared columns in file (default = fields)")
		fixedCols   = fs.Int("fixed-cols", 6, "fixed column count (always filled)")
		indexFlds   = fs.Int("index-fields", 15, "indexed declared columns (filled every row)")
		fill        = fs.Int("fill", 100, "filled declared columns per row")
		rowWidth    = fs.Int("row-width", 1024, "target row width in bytes")
		fillBuckets = fs.Int("fill-buckets", 0, ">0: cycle N pre-picked column sets")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesSpec)
	if err != nil {
		return err
	}
	if *segments == 0 || *totalRows == 0 || *outDir == "" {
		return errors.New("--segments, --total-rows and --out-dir are required")
	}
	cfg := gen.Config{TotalRows: *totalRows, Segments: *segments, Scatter: *scatter, GapFactor: *gapFactor,
		Seed: *seed, Fields: *fields, FileCols: *fileCols, FixedCols: *fixedCols, IndexFields: *indexFlds,
		Fill: *fill, RowWidth: *rowWidth, FillBuckets: *fillBuckets}
	if err := cfg.Validate(); err != nil {
		return err
	}
	n := uint64(len(nodes))
	if *segments < n {
		return fmt.Errorf("segments (%d) < nodes (%d)", *segments, n)
	}
	tasks := newTasks(30 * time.Minute)
	defer func() { err = errors.Join(err, tasks.close(err != nil)) }()

	type jobInfo struct {
		node Node
		from uint64
		to   uint64
		ids  []string
	}
	// Split segments evenly: node i gets [i*seg/n, (i+1)*seg/n).
	infos := make([]jobInfo, 0, n)
	for i, nd := range nodes {
		from := uint64(i) * *segments / n
		to := (uint64(i) + 1) * *segments / n
		if from >= to {
			continue
		}
		j := jobInfo{node: nd, from: from, to: to}
		infos = append(infos, j)
	}

	// Submit one request per node carrying its whole segment range; the
	// daemon is stateless, so every job request is self-contained.
	var wg sync.WaitGroup
	errCh := make(chan error, len(infos))
	for i := range infos {
		info := &infos[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seg := info.from; seg < info.to; seg++ {
				body := map[string]interface{}{
					"out":          fmt.Sprintf("%s/seg-%d.csv", *outDir, seg),
					"segment":      seg,
					"segments":     *segments,
					"total_rows":   *totalRows,
					"scatter":      *scatter,
					"gap_factor":   *gapFactor,
					"seed":         *seed,
					"fields":       *fields,
					"file_cols":    *fileCols,
					"fixed_cols":   *fixedCols,
					"index_fields": *indexFlds,
					"fill":         *fill,
					"row_width":    *rowWidth,
					"fill_buckets": *fillBuckets,
				}
				id, err := tasks.submit(info.node.Addr, "gen", body)
				if err != nil {
					errCh <- fmt.Errorf("%s: seg %d: %w", info.node.Addr, seg, err)
					tasks.stop()
					return
				}
				info.ids = append(info.ids, id)
				// Keep one generator active per node, independent of segment count.
				if err := tasks.wait(info.node.Addr, id); err != nil {
					errCh <- err
					tasks.stop()
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	fmt.Printf("generated %d segments on %d nodes (%s)\n",
		*segments, n, *outDir)
	return nil
}

// ---------- load ----------

func runLoad(args []string) (err error) {
	fs := flag.NewFlagSet("dist load", flag.ContinueOnError)
	var (
		nodesSpec = fs.String("nodes", "", `comma list "host:daemon_port[:sql_port]"`)
		dsn       = fs.String("dsn", "", `DSN template; {port} replaced by the node's sql_port`)
		table     = fs.String("table", "", "target table")
		dir       = fs.String("segments-dir", "", "segment directory on every node")
		workers   = fs.Int("workers", 16, "concurrent sessions per node")
		entry     = fs.String("entry", "upsert", "entry: csv | insert | upsert")
		batchRows = fs.Int("batch-rows", 2000, "insert entry: rows per INSERT statement")
		multiRow  = fs.Int("multi-row", 0, "insert entry: multi-row grouping (0=off)")
		prepared  = fs.Bool("prepared", false, "cache prepared statements")
		noResume  = fs.Bool("no-resume", false, "load every file again")
		bulk      = fs.Bool("bulk", false, "enable SST bulk load mode")
		allowSK   = fs.Bool("allow-sk", true, "with --bulk: allow secondary keys")
		unsorted  = fs.Bool("allow-unsorted", false, "with --bulk: input rows are unordered")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesSpec)
	if err != nil {
		return err
	}
	if *dsn == "" || *table == "" || *dir == "" {
		return errors.New("--dsn, --table and --segments-dir are required")
	}
	tasks := newTasks(30 * time.Minute)
	defer func() { err = errors.Join(err, tasks.close(err != nil)) }()

	ids := make(map[string]string) // addr -> job id
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))
	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			dsnFinal := strings.ReplaceAll(*dsn, "{port}", nd.SQLPort)
			if strings.Contains(*dsn, "{port}") && nd.SQLPort == "" {
				errCh <- fmt.Errorf("%s: SQL port is required by DSN", nd.Addr)
				return
			}
			dsnFinal = strings.ReplaceAll(dsnFinal, "{host}", nd.Host)
			body := map[string]interface{}{
				"dsn":            dsnFinal,
				"table":          *table,
				"segments_dir":   *dir,
				"workers":        *workers,
				"entry":          *entry,
				"batch_rows":     *batchRows,
				"multi_row":      *multiRow,
				"prepared":       *prepared,
				"no_resume":      *noResume,
				"bulk":           *bulk,
				"allow_sk":       *allowSK,
				"allow_unsorted": *unsorted,
			}
			id, err := tasks.submit(nd.Addr, "load", body)
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", nd.Addr, err)
				return
			}
			mu.Lock()
			ids[nd.Addr] = id
			mu.Unlock()
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	fmt.Printf("load started on %d nodes\n", len(ids))

	type agg struct {
		running int
		done    int
		errs    []string
		rows    int64
		rps     float64
	}
	sortNodes := make([]Node, 0, len(nodes))
	seen := map[string]bool{}
	for _, nd := range nodes {
		if _, ok := ids[nd.Addr]; ok && !seen[nd.Addr] {
			seen[nd.Addr] = true
			sortNodes = append(sortNodes, nd)
		}
	}
	start := time.Now()
	for time.Since(start) < 30*time.Minute {
		time.Sleep(2 * time.Second)
		a := agg{}
		for _, nd := range sortNodes {
			var j struct {
				Finished bool          `json:"finished"`
				Err      string        `json:"error"`
				Progress *jsonProgress `json:"progress"`
			}
			if err := httpJSONContext(tasks.ctx, nd.Addr, "GET", "/api/job/"+ids[nd.Addr], nil, &j); err != nil {
				return fmt.Errorf("%s: %w", nd.Addr, err)
			}
			if j.Err != "" {
				a.errs = append(a.errs, fmt.Sprintf("%s: %s", nd.Addr, j.Err))
			}
			if !j.Finished {
				a.running++
			} else {
				a.done++
			}
			if j.Progress != nil {
				a.rows += j.Progress.Rows
				a.rps += j.Progress.RowsPerSec
			}
		}
		el := time.Since(start).Seconds()
		fmt.Printf("[dist] %d/%d nodes done, %d rows, %.0f rows/s, elapsed %.0fs\n",
			a.done, len(sortNodes), a.rows, a.rps, el)
		if len(a.errs) > 0 {
			return fmt.Errorf("load failed: %s", strings.Join(a.errs, "; "))
		}
		if a.running == 0 {
			fmt.Printf("dist load finished: %d rows in %.0fs\n", a.rows, el)
			return nil
		}
	}
	return errors.New("distributed load timed out after 30 minutes")
}

type jsonProgress struct {
	SegmentsDone  int     `json:"segments_done"`
	SegmentsTotal int     `json:"segments_total"`
	Rows          int64   `json:"rows"`
	RowsPerSec    float64 `json:"rows_per_sec"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	Done          bool    `json:"done"`
}

// ---------- bench ----------

// runBench fans a bench workload out to all nodes and aggregates the
// combined read/write rate until every node finishes.
func runBench(args []string) (err error) {
	fs := flag.NewFlagSet("dist bench", flag.ContinueOnError)
	var (
		nodesSpec = fs.String("nodes", "", `comma list "host:daemon_port[:sql_port]"`)
		dsn       = fs.String("dsn", "", `DSN template; {port} replaced by the node's sql_port`)
		table     = fs.String("table", "", "target table")
		threads   = fs.Int("threads", 16, "concurrent client threads per node")
		duration  = fs.Int("duration", 600, "run duration in seconds")
		readRatio = fs.Float64("read-ratio", 0.2, "read fraction 0..1 (customer mix ~0.2)")
		segments  = fs.Uint64("segments", 0, "segment count the data was generated with")
		totalRows = fs.Uint64("total-rows", 0, "total rows the data was generated with")
		gapFactor = fs.Uint64("gap-factor", 10, "blocked: segment stride = rows-per-segment * gap-factor")
		scatter   = fs.String("scatter", "hash32", "key layout used by gen: hash32 (customer scatter, default) | blocked | none | mul-hash")
		fixedCols = fs.Int("fixed-cols", 6, "fixed f-column count (same as gen)")
		fill      = fs.Int("fill", 200, "columns written per split row (customer ~200 filled)")
		rowWidth  = fs.Int("row-width", 1024, "target row width in bytes")
		seed      = fs.Uint64("seed", 1, "random seed (per-node seeds derived from it)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesSpec)
	if err != nil {
		return err
	}
	if *dsn == "" || *table == "" {
		return errors.New("--dsn and --table are required")
	}
	if *segments == 0 || *totalRows == 0 {
		return errors.New("--segments and --total-rows are required (match the gen run)")
	}
	tasks := newTasks(time.Duration(*duration+30) * time.Second)
	defer func() { err = errors.Join(err, tasks.close(err != nil)) }()

	ids := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))
	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			dsnFinal := strings.ReplaceAll(*dsn, "{port}", nd.SQLPort)
			if strings.Contains(*dsn, "{port}") && nd.SQLPort == "" {
				errCh <- fmt.Errorf("%s: SQL port is required by DSN", nd.Addr)
				return
			}
			dsnFinal = strings.ReplaceAll(dsnFinal, "{host}", nd.Host)
			body := map[string]interface{}{
				"dsn":        dsnFinal,
				"table":      *table,
				"threads":    *threads,
				"duration_s": *duration,
				"read_ratio": *readRatio,
				"segments":   *segments,
				"total_rows": *totalRows,
				"gap_factor": *gapFactor,
				"scatter":    *scatter,
				"fixed_cols": *fixedCols,
				"fill":       *fill,
				"row_width":  *rowWidth,
				"seed":       *seed,
			}
			id, err := tasks.submit(nd.Addr, "bench", body)
			if err != nil {
				errCh <- fmt.Errorf("%s: %w", nd.Addr, err)
				return
			}
			mu.Lock()
			ids[nd.Addr] = id
			mu.Unlock()
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	fmt.Printf("bench started on %d nodes (%ds, read-ratio %.2f)\n", len(ids), *duration, *readRatio)

	type rep struct {
		ElapsedS   float64 `json:"elapsed_s"`
		Reads      int64   `json:"reads"`
		Writes     int64   `json:"writes"`
		ReadRPS    float64 `json:"read_rps"`
		WriteRPS   float64 `json:"write_rps"`
		Errors     int64   `json:"errors"`
		WriteP99MS float64 `json:"write_p99_ms"`
		WriteP50MS float64 `json:"write_p50_ms"`
	}
	start := time.Now()
	for time.Since(start) < time.Duration(*duration+30)*time.Second {
		time.Sleep(2 * time.Second)
		running := 0
		var totR, totW, errs int64
		var rpsSum, wpsSum, p99max, p50max float64
		for _, nd := range nodes {
			var j struct {
				Finished bool            `json:"finished"`
				Err      string          `json:"error"`
				Progress json.RawMessage `json:"progress"`
			}
			if err := httpJSONContext(tasks.ctx, nd.Addr, "GET", "/api/job/"+ids[nd.Addr], nil, &j); err != nil {
				return err
			}
			if j.Err != "" {
				return fmt.Errorf("%s: %s", nd.Addr, j.Err)
			}
			if len(j.Progress) > 0 {
				var r rep
				if json.Unmarshal(j.Progress, &r) == nil {
					totR += r.Reads
					totW += r.Writes
					errs += r.Errors
					rpsSum += r.ReadRPS
					wpsSum += r.WriteRPS
					if r.WriteP99MS > p99max {
						p99max = r.WriteP99MS
					}
					if r.WriteP50MS > p50max {
						p50max = r.WriteP50MS
					}
				}
			}
			if !j.Finished {
				running++
			}
		}
		el := time.Since(start).Seconds()
		fmt.Printf("[dist bench] %.0fs reads %d (%.0f/s) writes %d (%.0f/s) errs %d p99max %.1fms\n",
			el, totR, rpsSum, totW, wpsSum, errs, p99max)
		if running == 0 {
			fmt.Printf("bench finished: reads %d, writes %d, p50 %.1fms, p99 %.1fms\n",
				totR, totW, p50max, p99max)
			return nil
		}
	}
	return errors.New("distributed bench timed out")
}

// ---------- stats ----------

func runStats(args []string) error {
	fs := flag.NewFlagSet("dist stats", flag.ContinueOnError)
	nodesSpec := fs.String("nodes", "", `comma list "host:daemon_port[:sql_port]"`)
	sec := fs.Float64("sec", 2, "sampling interval per node (1..10)")
	out := fs.String("out", "", "controller-side file to append one JSON line per sample (default: print only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesSpec)
	if err != nil {
		return err
	}
	type row struct {
		node Node
		st   struct {
			CPUBusyPct float64 `json:"cpu_busy_pct"`
			CPUs       int     `json:"cpus"`
			LoadAvg1   float64 `json:"load_avg_1"`
			MemAvailKB uint64  `json:"mem_avail_kb"`
			DiskIOPS   float64 `json:"disk_iops"`
			DiskKBs    float64 `json:"disk_kb_per_sec"`
		}
		err error
	}
	rows := make([]row, len(nodes))
	var wg sync.WaitGroup
	for i, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows[i].node = nd
			path := fmt.Sprintf("/api/host-stats?sec=%s", strconv.FormatFloat(*sec, 'f', -1, 64))
			rows[i].err = httpJSON(nd.Addr, "GET", path, nil, &rows[i].st)
		}()
	}
	wg.Wait()
	if *out != "" {
		type snapRow struct {
			Addr       string  `json:"addr"`
			CPUBusyPct float64 `json:"cpu_busy_pct"`
			CPUs       int     `json:"cpus"`
			LoadAvg1   float64 `json:"load_avg_1"`
			MemAvailKB uint64  `json:"mem_avail_kb"`
			DiskIOPS   float64 `json:"disk_iops"`
			DiskKBs    float64 `json:"disk_kb_per_sec"`
			Err        string  `json:"error,omitempty"`
		}
		snaps := make([]snapRow, 0, len(rows))
		for _, r := range rows {
			sr := snapRow{
				Addr:       r.node.Addr,
				CPUBusyPct: r.st.CPUBusyPct,
				CPUs:       r.st.CPUs,
				LoadAvg1:   r.st.LoadAvg1,
				MemAvailKB: r.st.MemAvailKB,
				DiskIOPS:   r.st.DiskIOPS,
				DiskKBs:    r.st.DiskKBs,
			}
			if r.err != nil {
				sr.Err = r.err.Error()
			}
			snaps = append(snaps, sr)
		}
		line := struct {
			Time time.Time `json:"time"`
			Rows []snapRow `json:"nodes"`
		}{time.Now(), snaps}
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		err = json.NewEncoder(f).Encode(line)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	fmt.Printf("%-10s %-10s %-8s %-8s %-12s %-10s %-12s\n",
		"node", "cpu_busy%", "cpus", "load1", "mem_availGB", "disk_iops", "disk_MB/s")
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].node.Name < rows[j].node.Name })
	for _, r := range rows {
		if r.err != nil {
			return fmt.Errorf("%s: %w", r.node.Addr, r.err)
		}
		fmt.Printf("%-10s %-10.1f %-8d %-8.2f %-12.1f %-10.0f %-12.1f\n",
			r.node.Addr, r.st.CPUBusyPct, r.st.CPUs, r.st.LoadAvg1,
			float64(r.st.MemAvailKB)/1024/1024, r.st.DiskIOPS, r.st.DiskKBs/1024)
	}
	return nil
}

// ---------- stop ----------

func runStop(args []string) error {
	fs := flag.NewFlagSet("dist stop", flag.ContinueOnError)
	nodesSpec := fs.String("nodes", "", `comma list "host:daemon_port[:sql_port]"`)
	jobID := fs.String("job", "", "stop one job id on all nodes (default: all running jobs)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesSpec)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodes))
	for _, nd := range nodes {
		nd := nd
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *jobID != "" {
				if err := httpJSON(nd.Addr, "POST", "/api/stop/"+*jobID, nil, nil); err != nil {
					errCh <- fmt.Errorf("%s: %w", nd.Addr, err)
				}
				return
			}
			var jobs []struct {
				ID       string `json:"id"`
				Finished bool   `json:"finished"`
			}
			if err := httpJSON(nd.Addr, "GET", "/api/jobs", nil, &jobs); err != nil {
				errCh <- fmt.Errorf("%s: %w", nd.Addr, err)
				return
			}
			for _, j := range jobs {
				if !j.Finished {
					if err := httpJSON(nd.Addr, "POST", "/api/stop/"+j.ID, nil, nil); err != nil {
						errCh <- fmt.Errorf("%s: %w", nd.Addr, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	fmt.Println("stop requests sent")
	return nil
}
