package dist

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeValidation(t *testing.T) {
	for _, spec := range []string{"", "host", "host:0", ":80", "host:80,host:80", "host:80:no"} {
		if _, err := parseNodes(spec); err == nil {
			t.Fatalf("invalid nodes accepted: %q", spec)
		}
	}
	if nodes, err := parseNodes("a:9900:3306,b:9901:3307"); err != nil || len(nodes) != 2 {
		t.Fatal(nodes, err)
	}
}

func TestCurrentGenerationIgnoresOldFailures(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/gen", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": r.Header.Get("X-Job-ID")})
	})
	mux.HandleFunc("/api/job/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"finished": true})
	})
	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
		t.Error("generation queried unrelated jobs")
		json.NewEncoder(w).Encode([]map[string]any{{"kind": "gen", "finished": true, "error": "old failure"}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	err := runGen([]string{"-nodes", strings.TrimPrefix(server.URL, "http://"),
		"-segments", "1", "-total-rows", "2", "-out-dir", t.TempDir(),
		"-fields", "5", "-fixed-cols", "0", "-index-fields", "1", "-fill", "3"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBenchFailureIsReturned(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/bench", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": r.Header.Get("X-Job-ID")})
	})
	mux.HandleFunc("/api/job/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"finished": true, "error": "database failure"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	err := runBench([]string{"-nodes", strings.TrimPrefix(server.URL, "http://"),
		"-dsn", "u:p@tcp(localhost:3306)/db", "-table", "t",
		"-segments", "1", "-total-rows", "2", "-duration", "1"})
	if err == nil || !strings.Contains(err.Error(), "database failure") {
		t.Fatal("job error not returned:", err)
	}
}

func TestSubmissionFailureStopsStartedJobs(t *testing.T) {
	stopped := false
	mux := http.NewServeMux()
	mux.HandleFunc("/api/load", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": r.Header.Get("X-Job-ID")})
	})
	mux.HandleFunc("/api/job/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"finished": stopped})
	})
	mux.HandleFunc("/api/stop/", func(w http.ResponseWriter, r *http.Request) {
		stopped = true
		json.NewEncoder(w).Encode(map[string]string{"status": "stop requested"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	reject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rejected", 400)
	}))
	defer reject.Close()
	err := runLoad([]string{"-nodes", strings.TrimPrefix(server.URL, "http://") + "," + strings.TrimPrefix(reject.URL, "http://"),
		"-dsn", "u:p@tcp(localhost:3306)/db", "-table", "t", "-segments-dir", "."})
	if err == nil || !stopped {
		t.Fatal("failure did not cancel started job:", err, stopped)
	}
}

func TestLostSubmissionResponseStillStopsJob(t *testing.T) {
	var accepted, stopped atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/gen", func(w http.ResponseWriter, r *http.Request) {
		accepted.Store(r.Header.Get("X-Job-ID") != "")
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	})
	mux.HandleFunc("/api/job/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]bool{"finished": stopped.Load()})
	})
	mux.HandleFunc("/api/stop/", func(w http.ResponseWriter, r *http.Request) {
		stopped.Store(true)
		json.NewEncoder(w).Encode(map[string]string{"status": "stop requested"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	tasks := newTasks(time.Second)
	if _, err := tasks.submit(strings.TrimPrefix(server.URL, "http://"), "gen", map[string]string{}); err == nil {
		t.Fatal("lost response accepted")
	}
	if err := tasks.close(true); err != nil || !accepted.Load() || !stopped.Load() {
		t.Fatal("job left running after lost response:", err)
	}
}

func TestTaskDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"finished": false})
	}))
	defer server.Close()
	tasks := newTasks(10 * time.Millisecond)
	defer tasks.close(false)
	if err := tasks.wait(strings.TrimPrefix(server.URL, "http://"), "active"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
