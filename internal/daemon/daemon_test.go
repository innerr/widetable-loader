package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/innerr/widetable-loader/internal/gen"
)

func TestAPIAndConcurrentPolling(t *testing.T) {
	s := &Server{jobs: map[string]*Job{}}
	server := httptest.NewServer(s.handler())
	defer server.Close()
	dir := t.TempDir()
	body, _ := json.Marshal(GenJob{Out: filepath.Join(dir, "seg.csv"), TotalRows: 24, Segments: 2, Fields: 5, Fill: 3})
	resp, err := http.Post(server.URL+"/api/gen", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var started map[string]string
	json.NewDecoder(resp.Body).Decode(&started)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode, started)
	}
	for i := 0; i < 30; i++ {
		resp, err := http.Get(server.URL + "/api/job/" + started["id"])
		if err != nil {
			t.Fatal(err)
		}
		var job Job
		json.NewDecoder(resp.Body).Decode(&job)
		resp.Body.Close()
		if job.Finished {
			if job.Err != "" {
				t.Fatal(job.Err)
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	s.wg.Wait()
	if _, err := os.Stat(filepath.Join(dir, "seg.csv.columns")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/load", "/api/bench", "/api/gen"} {
		resp, err := http.Post(server.URL+path, "application/json", bytes.NewBufferString("{}"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatal(path, resp.StatusCode)
		}
	}
	resp, _ = http.Get(server.URL + "/api/gen")
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Fatal("wrong method accepted")
	}
	resp, err = http.Post(server.URL+"/api/gen", "application/json", bytes.NewBufferString(string(body)+"{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal("extra JSON object accepted")
	}
	for _, sec := range []string{"NaN", "-1", "20", "bad"} {
		resp, err := http.Get(server.URL + "/api/host-stats?sec=" + sec)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatal("invalid sampling interval accepted:", sec)
		}
	}
}

func TestGenJobUsesCommonPairValidation(t *testing.T) {
	validJob := func() GenJob {
		return GenJob{TotalRows: 10, Segments: 1, Scatter: "hash32", Format: "sql", SQLTable: "t", SQLMultiRow: 1,
			PairCatalog: 100, PairFill: 10, PairRawBytes: []gen.PairRaw{{Bytes: 96, Weight: 1}}, PairTimeSpan: 1,
			PairIndex: gen.PairIndex{DWDistinct: 100, RowsPerPgID: 250, RowsPerOssPath: 4,
				RowsPerAnonymousID: 1000, RowsPerAttachment: 4, RowsPerPackageName: 100, RowsPerParentID: 1000}}
	}
	for _, test := range []struct {
		name   string
		mutate func(*GenJob)
		want   string
	}{
		{name: "fields", mutate: func(j *GenJob) { j.Fields = 1 }, want: "fields"},
		{name: "file cols", mutate: func(j *GenJob) { j.FileCols = 1 }, want: "file-cols"},
		{name: "fixed cols", mutate: func(j *GenJob) { j.FixedCols = 1 }, want: "fixed-cols"},
		{name: "index fields", mutate: func(j *GenJob) { j.IndexFields = 1 }, want: "index-fields"},
		{name: "fill", mutate: func(j *GenJob) { j.Fill = 1 }, want: "fill"},
		{name: "row width", mutate: func(j *GenJob) { j.RowWidth = 1 }, want: "row-width"},
		{name: "gap outside blocked", mutate: func(j *GenJob) { j.GapFactor = 5 }, want: "gap-factor"},
		{name: "multi row without buckets", mutate: func(j *GenJob) { j.SQLMultiRow = 50 }, want: "fill-buckets"},
		{name: "bucket rows without buckets", mutate: func(j *GenJob) { j.PairBucketRows = 50 }, want: "fill-buckets"},
		{name: "upsert", mutate: func(j *GenJob) { j.SQLUpsert = true }, want: "sql-upsert"},
		{name: "max row bytes is mapped", mutate: func(j *GenJob) { j.PairMaxRowBytes = 1 }, want: "pair-max-row-bytes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			test.mutate(&job)
			if _, err := gen.NewGenerator(job.config(), 0); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want one mentioning %q", err, test.want)
			}
		})
	}
	if _, err := gen.NewGenerator(validJob().config(), 0); err != nil {
		t.Fatalf("zero-valued generic JSON fields must not count as explicit: %v", err)
	}
}

func TestGenJobUsesCommonSQLValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*GenJob)
		want   string
	}{
		{name: "table with csv", mutate: func(j *GenJob) { j.SQLTable = "t" }, want: "format sql"},
		{name: "multi row with csv", mutate: func(j *GenJob) { j.SQLMultiRow = 2 }, want: "format sql"},
		{name: "upsert with csv", mutate: func(j *GenJob) { j.SQLUpsert = true }, want: "format sql"},
		{name: "sql without table", mutate: func(j *GenJob) { j.Format = "sql" }, want: "sql-table"},
		{name: "bucket rows without pair", mutate: func(j *GenJob) { j.PairBucketRows = 2 }, want: "pair-catalog"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := GenJob{TotalRows: 2, Segments: 1, Scatter: "none", Format: "csv", SQLMultiRow: 1, Fields: 2, Fill: 1}
			test.mutate(&job)
			if _, err := gen.NewGenerator(job.config(), 0); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want one mentioning %q", err, test.want)
			}
		})
	}
}

func TestStopActiveJobAndRejectShutdownRequests(t *testing.T) {
	s := &Server{jobs: map[string]*Job{}}
	started := make(chan struct{})
	w := httptest.NewRecorder()
	s.launch(w, "gen", "", func(ctx context.Context, report func(any)) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	var response map[string]string
	json.NewDecoder(w.Body).Decode(&response)
	s.stopJob(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/stop/"+response["id"], nil))
	s.wg.Wait()
	if !s.jobs[response["id"]].Finished || s.jobs[response["id"]].Err != context.Canceled.Error() {
		t.Fatal(s.jobs)
	}
	s.cancelJobs()
	w = httptest.NewRecorder()
	s.launch(w, "gen", "", func(ctx context.Context, report func(any)) error {
		t.Error("job launched during shutdown")
		return nil
	})
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestCallerSuppliedJobID(t *testing.T) {
	s := &Server{jobs: map[string]*Job{}}
	id := "0123456789abcdef0123456789abcdef"
	w := httptest.NewRecorder()
	s.launch(w, "gen", id, func(ctx context.Context, report func(any)) error { return nil })
	s.wg.Wait()
	if s.jobs[id] == nil || !s.jobs[id].Finished {
		t.Fatal("caller id not retained")
	}
	w = httptest.NewRecorder()
	s.launch(w, "gen", id, func(ctx context.Context, report func(any)) error {
		t.Error("duplicate job executed")
		return nil
	})
	if w.Code != 409 {
		t.Fatal("duplicate accepted:", w.Code)
	}
}
func TestCanceledGenerationLeavesNoFiles(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runGen(ctx, GenJob{Out: filepath.Join(dir, "out.csv"), TotalRows: 4, Segments: 1, Fields: 2, Fill: 1})
	if err == nil {
		t.Fatal("canceled generation succeeded")
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("partial files left behind")
	}
}
