// Package daemon runs independent generation, import and workload jobs.
package daemon

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/innerr/widetable-loader/internal/bench"
	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/load"
)

type GenJob struct {
	Out         string `json:"out"`
	Segment     uint64 `json:"segment"`
	Segments    uint64 `json:"segments"`
	TotalRows   uint64 `json:"total_rows"`
	Scatter     string `json:"scatter"`
	GapFactor   uint64 `json:"gap_factor"`
	Seed        uint64 `json:"seed"`
	Fields      int    `json:"fields"`
	FileCols    int    `json:"file_cols"`
	FixedCols   int    `json:"fixed_cols"`
	IndexFields int    `json:"index_fields"`
	Fill        int    `json:"fill"`
	RowWidth    int    `json:"row_width"`
	FillBuckets int    `json:"fill_buckets"`
	Format      string `json:"format"`           // csv (default) | sql
	SQLTable    string `json:"sql_table"`        // sql: target table name
	SQLMultiRow int    `json:"sql_multi_row"`    // sql: rows per INSERT statement
	SQLUpsert   bool   `json:"sql_upsert"`       // sql: append ON DUPLICATE KEY UPDATE
}

func (this GenJob) config() gen.Config {
	scatter := this.Scatter
	if scatter == "" {
		scatter = "hash32"
	}
	gap := this.GapFactor
	if gap == 0 {
		gap = 10
	}
	return gen.Config{TotalRows: this.TotalRows, Segments: this.Segments, Scatter: scatter,
		GapFactor: gap, Seed: this.Seed, Fields: this.Fields, FileCols: this.FileCols,
		FixedCols: this.FixedCols, IndexFields: this.IndexFields, Fill: this.Fill,
		RowWidth: this.RowWidth, FillBuckets: this.FillBuckets}
}

type Job struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	StartedAt time.Time       `json:"started_at"`
	Finished  bool            `json:"finished"`
	Err       string          `json:"error,omitempty"`
	Progress  json.RawMessage `json:"progress,omitempty"`
	cancel    context.CancelFunc
}

type Server struct {
	mu      sync.Mutex
	jobs    map[string]*Job
	wg      sync.WaitGroup
	closing bool
}

func (this *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/gen", this.startGen)
	mux.HandleFunc("POST /api/load", this.startLoad)
	mux.HandleFunc("POST /api/bench", this.startBench)
	mux.HandleFunc("GET /api/job/", this.jobStatus)
	mux.HandleFunc("GET /api/jobs", this.listJobs)
	mux.HandleFunc("POST /api/stop/", this.stopJob)
	mux.HandleFunc("GET /api/host-stats", this.handleHostStats)
	return mux
}

func Run(args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	listen := fs.String("listen", ":9900", "HTTP listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s := &Server{jobs: map[string]*Job{}}
	srv := &http.Server{Addr: *listen, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		s.cancelJobs()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	fmt.Fprintf(os.Stderr, "daemon listening on %s\n", *listen)
	err := srv.ListenAndServe()
	s.wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (this *Server) cancelJobs() {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.closing = true
	for _, job := range this.jobs {
		job.cancel()
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	reader := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	reader.DisallowUnknownFields()
	if err := reader.Decode(out); err != nil {
		writeErr(w, 400, err.Error())
		return false
	}
	if err := reader.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeErr(w, 400, "request must contain exactly one JSON object")
		return false
	}
	return true
}

func (this *Server) launch(w http.ResponseWriter, kind, id string, run func(context.Context, func(any)) error) {
	if id == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		id = hex.EncodeToString(b)
	} else if b, err := hex.DecodeString(id); err != nil || len(b) != 16 {
		writeErr(w, 400, "X-Job-ID must contain 32 hexadecimal characters")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{ID: id, Kind: kind, StartedAt: time.Now(), cancel: cancel}
	this.mu.Lock()
	if _, exists := this.jobs[id]; exists {
		this.mu.Unlock()
		cancel()
		writeErr(w, 409, "job id already exists")
		return
	}
	if this.closing {
		this.mu.Unlock()
		cancel()
		writeErr(w, 503, "daemon is stopping")
		return
	}
	this.jobs[j.ID] = j
	this.wg.Add(1)
	this.mu.Unlock()
	go func() {
		defer this.wg.Done()
		defer cancel()
		err := run(ctx, func(progress any) {
			b, err := json.Marshal(progress)
			if err != nil {
				return
			}
			this.mu.Lock()
			j.Progress = b
			this.mu.Unlock()
		})
		this.mu.Lock()
		j.Finished = true
		if err != nil {
			j.Err = err.Error()
		}
		this.mu.Unlock()
	}()
	writeJSON(w, 200, map[string]string{"id": j.ID, "status": "started"})
}

func (this *Server) startGen(w http.ResponseWriter, r *http.Request) {
	var p GenJob
	if !decode(w, r, &p) {
		return
	}
	if p.Out == "" {
		writeErr(w, 400, "out is required")
		return
	}
	if p.Format == "" {
		p.Format = "csv"
	}
	if p.Format != "csv" && p.Format != "sql" {
		writeErr(w, 400, "format must be csv or sql")
		return
	}
	if p.Format == "csv" && (p.SQLTable != "" || p.SQLMultiRow != 0 || p.SQLUpsert) {
		writeErr(w, 400, "sql_* fields require format=sql")
		return
	}
	if p.Format == "sql" {
		if p.SQLTable == "" {
			writeErr(w, 400, "sql format requires sql_table")
			return
		}
		if p.SQLMultiRow < 1 {
			p.SQLMultiRow = 1
		}
	}
	if _, err := gen.NewGenerator(p.config(), p.Segment); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	this.launch(w, "gen", r.Header.Get("X-Job-ID"), func(ctx context.Context, report func(any)) error { return runGen(ctx, p) })
}

func (this *Server) startLoad(w http.ResponseWriter, r *http.Request) {
	var p load.Params
	if !decode(w, r, &p) {
		return
	}
	if p.Entry == "" {
		p.Entry = "upsert"
	}
	if p.Workers == 0 {
		p.Workers = 1
	}
	if p.BatchRows == 0 {
		p.BatchRows = 1000
	}
	if err := p.Validate(); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	this.launch(w, "load", r.Header.Get("X-Job-ID"), func(ctx context.Context, report func(any)) error {
		return load.RunWithProgress(ctx, p, func(pr load.Progress) { report(pr) })
	})
}

func (this *Server) startBench(w http.ResponseWriter, r *http.Request) {
	var p bench.Params
	if !decode(w, r, &p) {
		return
	}
	if err := p.Validate(); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	this.launch(w, "bench", r.Header.Get("X-Job-ID"), func(ctx context.Context, report func(any)) error {
		return bench.RunWithReport(ctx, p, func(pr bench.Report) { report(pr) })
	})
}

func (this *Server) jobStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/job/")
	this.mu.Lock()
	defer this.mu.Unlock()
	j, ok := this.jobs[id]
	if !ok {
		writeErr(w, 404, "no such job")
		return
	}
	writeJSON(w, 200, j)
}

func (this *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	this.mu.Lock()
	defer this.mu.Unlock()
	jobs := make([]*Job, 0, len(this.jobs))
	for _, j := range this.jobs {
		jobs = append(jobs, j)
	}
	writeJSON(w, 200, jobs)
}

func (this *Server) stopJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/stop/")
	this.mu.Lock()
	defer this.mu.Unlock()
	j, ok := this.jobs[id]
	if !ok {
		writeErr(w, 404, "no such job")
		return
	}
	j.cancel()
	writeJSON(w, 200, map[string]string{"id": id, "status": "stop requested"})
}

func runGen(ctx context.Context, j GenJob) error {
	cfg := j.config()
	g, err := gen.NewGenerator(cfg, j.Segment)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(j.Out), 0o755); err != nil {
		return err
	}
	ext := ".csv"
	if j.Format == "sql" {
		ext = ".sql"
	}
	f, err := os.CreateTemp(filepath.Dir(j.Out), ".generation-*"+ext)
	if err != nil {
		return err
	}
	stage := f.Name()
	defer os.Remove(stage)
	w := bufio.NewWriterSize(f, 1<<20)
	columns := true
	if j.Format == "sql" {
		err = g.WriteSQLContext(ctx, w, j.SQLTable, j.SQLMultiRow, j.SQLUpsert)
		columns = false
	} else {
		err = g.WriteCSVContext(ctx, w)
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if columns {
		if err := os.WriteFile(stage+".columns", []byte(strings.Join(cfg.ColumnNames(), "\n")+"\n"), 0o644); err != nil {
			return err
		}
		defer os.Remove(stage + ".columns")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return gen.PublishSegment(stage, j.Out, columns)
}
