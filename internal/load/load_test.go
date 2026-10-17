package load

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidationAndSQL(t *testing.T) {
	p := Params{DSN: "u:p@tcp(localhost:3306)/db", Table: "items", Dir: ".", Workers: 1, BatchRows: 2, Entry: "upsert"}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.Workers = 0
	if p.Validate() == nil {
		t.Fatal("zero workers accepted")
	}
	stmt := insertSQL("my-db.order", []string{"`id`", "`value`"}, 2, true)
	if !strings.Contains(stmt, "ON DUPLICATE KEY UPDATE") || !strings.Contains(stmt, "`my-db`.`order`") ||
		strings.Count(stmt, "?") != 4 {
		t.Fatal(stmt)
	}
	names, args := filledRow([]string{"1", `\N`, ""}, []string{"id", "a", "b"})
	if len(names) != 2 || args[1] != "" {
		t.Fatal("NULL and empty string confused")
	}
}

func TestStatementRows(t *testing.T) {
	for _, c := range []struct {
		stmt string
		want int
	}{{"INSERT INTO t VALUES (1)", 1},
		{"INSERT INTO t VALUES (1),(2),(3)", 3},
		{"INSERT INTO t VALUES ('a'),(\"b\"),(c),(',(')", 4}} {
		if got := statementRows(c.stmt); got != c.want {
			t.Fatalf("statementRows(%q) = %d, want %d", c.stmt, got, c.want)
		}
	}
}

func TestMixedSegmentFormatsRejected(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.sql"), []byte("INSERT;\n"), 0o600)
	p := Params{DSN: "u:p@tcp(localhost:3306)/db", Table: "items", Dir: dir, Workers: 1, BatchRows: 1, Entry: "insert"}
	if err := RunWithProgress(context.Background(), p, nil); err == nil || !strings.Contains(err.Error(), "mixed") {
		t.Fatal("mixed segment formats silently accepted:", err)
	}
}

func TestMissingSegmentRejectsResume(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\n"), 0o600)
	p := Params{DSN: "u:p@tcp(localhost:3306)/db", Table: "items", Dir: dir, Workers: 1, BatchRows: 1, Entry: "upsert"}
	state := manifest{Target: "u@tcp(localhost:3306)/db/items", Files: map[string]completed{"missing.csv": {Hash: "hash", Rows: 2}}}
	if err := saveManifest(filepath.Join(dir, ".load-progress.json"), state); err != nil {
		t.Fatal(err)
	}
	if err := RunWithProgress(context.Background(), p, nil); err == nil || !strings.Contains(err.Error(), "segment is missing") {
		t.Fatal("missing segment silently skipped:", err)
	}
}

func TestImportModes(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, mode := range []struct {
		name     string
		prepared bool
		multi    int
		entry    string
		bulk     bool
		indexed  bool
		unsorted bool
	}{
		{"insert", false, 0, "insert", false, false, false},
		{"upsert", false, 0, "upsert", false, false, false},
		{"prepared", true, 0, "upsert", false, false, false},
		{"multi", false, 3, "upsert", false, false, false},
		{"csv", false, 0, "csv", false, false, false},
		{"bulk", false, 2, "insert", true, false, false},
		{"bulk_indexed_unsorted", false, 2, "insert", true, true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			table := "loader_check_" + mode.name
			if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE `" + table + "` (id BIGINT PRIMARY KEY,a TEXT,b INT) ENGINE=WideTable"); err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TABLE `" + table + "`")
			if mode.indexed {
				if _, err := db.Exec("CREATE INDEX idx_b ON `" + table + "` (b)"); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			path := filepath.Join(dir, "seg.csv")
			data := "1,\"quote' and \\ slash\",7\n2,\"\",\\N\n3,\"three\",3\n4,\"four\",4\n"
			if mode.unsorted {
				data = "3,three,3\n4,four,4\n1,\"quote' and \\ slash\",7\n2,\"\",\\N\n"
			}
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+".columns", []byte("id\na\nb\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			p := Params{DSN: dsn, Table: table, Dir: dir, Workers: 2, BatchRows: 2, Entry: mode.entry,
				Prepared: mode.prepared, MultiRow: mode.multi, Bulk: mode.bulk, AllowSK: mode.indexed, Unsorted: mode.unsorted}
			var final Progress
			if err := RunWithProgress(ctx, p, func(pr Progress) { final = pr }); err != nil {
				t.Fatal(err)
			}
			if !final.Done || final.Rows != 4 {
				t.Fatal(final)
			}
			if err := RunWithProgress(ctx, p, nil); err != nil {
				t.Fatal("resume:", err)
			}
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM `" + table + "`").Scan(&count); err != nil || count != 4 {
				t.Fatal(count, err)
			}
			var first string
			if err := db.QueryRow("SELECT a FROM `" + table + "` WHERE id=1").Scan(&first); err != nil ||
				first != "quote' and \\ slash" {
				t.Fatalf("string changed: %q, error=%v", first, err)
			}
			var empty string
			var nullable sql.NullInt64
			if err := db.QueryRow("SELECT a,b FROM `"+table+"` WHERE id=2").Scan(&empty, &nullable); err != nil ||
				empty != "" || nullable.Valid {
				t.Fatalf("NULL/empty mismatch: %q %+v, error=%v", empty, nullable, err)
			}
			if mode.indexed {
				var id int64
				if err := db.QueryRow("SELECT id FROM `" + table + "` WHERE b=7").Scan(&id); err != nil || id != 1 {
					t.Fatal("bulk index query:", id, err)
				}
			}
			if mode.entry == "upsert" {
				if _, err := db.Exec("UPDATE `" + table + "` SET a='wrong' WHERE id=4"); err != nil {
					t.Fatal(err)
				}
				p.NoResume = true
				if err := RunWithProgress(ctx, p, nil); err != nil {
					t.Fatal("upsert replay:", err)
				}
				var got string
				if err := db.QueryRow("SELECT a FROM `" + table + "` WHERE id=4").Scan(&got); err != nil || got != "four" {
					t.Fatal(got, err)
				}
			}
			// A changed completed segment cannot silently inherit progress.
			os.WriteFile(path, []byte(data+"5,five,5\n"), 0o600)
			p.NoResume = false
			if err := RunWithProgress(ctx, p, nil); err == nil {
				t.Fatal("changed data skipped")
			}
		})
	}
}

func TestFailedBatchRollsBack(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE loader_rollback (id BIGINT PRIMARY KEY,a INT) ENGINE=WideTable"); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE loader_rollback")
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1,2\n1,3\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\na\n"), 0o600)
	p := Params{DSN: dsn, Table: "loader_rollback", Dir: dir, Workers: 1, BatchRows: 2, Entry: "insert"}
	if err := RunWithProgress(context.Background(), p, nil); err == nil {
		t.Fatal("duplicate insert succeeded")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM loader_rollback").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed transaction left data:", count, err)
	}
	b, err := os.ReadFile(filepath.Join(dir, ".load-progress.json"))
	if err != nil || !strings.Contains(string(b), `"inflight"`) || strings.Contains(string(b), `"rows":`) {
		t.Fatal("failed segment not marked uncertain:", string(b), err)
	}
	if err := RunWithProgress(context.Background(), p, nil); err == nil || !strings.Contains(err.Error(), "unfinished segments") {
		t.Fatal("unsafe insert replay accepted:", err)
	}
}

func TestPartialSegmentReplay(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE loader_partial_replay (id BIGINT PRIMARY KEY,a INT) ENGINE=WideTable"); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE loader_partial_replay")
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1,2\n2,3\n2,4\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\na\n"), 0o600)
	p := Params{DSN: dsn, Table: "loader_partial_replay", Dir: dir, Workers: 1, BatchRows: 1, Entry: "insert"}
	if err := RunWithProgress(context.Background(), p, nil); err == nil {
		t.Fatal("duplicate accepted")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM loader_partial_replay").Scan(&count); err != nil || count != 2 {
		t.Fatal("expected two committed batches:", count, err)
	}
	if err := RunWithProgress(context.Background(), p, nil); err == nil || !strings.Contains(err.Error(), "unfinished segments") {
		t.Fatal("unsafe replay accepted:", err)
	}
	p.Entry = "upsert"
	if err := RunWithProgress(context.Background(), p, nil); err != nil {
		t.Fatal("safe upsert replay:", err)
	}
}

func TestCSVWarningRollsBack(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE loader_csv_warning (id BIGINT PRIMARY KEY,a INT) ENGINE=WideTable"); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE loader_csv_warning")
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1,2\n1,3\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\na\n"), 0o600)
	p := Params{DSN: dsn, Table: "loader_csv_warning", Dir: dir, Workers: 1, BatchRows: 2, Entry: "csv"}
	if err := RunWithProgress(context.Background(), p, nil); err == nil {
		t.Fatal("CSV duplicate silently accepted")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM loader_csv_warning").Scan(&count); err != nil || count != 0 {
		t.Fatal("warning left data:", count, err)
	}
}

func TestDynamicColumnEntrypoints(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, entry := range []string{"insert", "csv"} {
		t.Run(entry, func(t *testing.T) {
			table := "loader_dynamic_" + entry
			if _, err := db.Exec("CREATE TABLE `" + table + "` (id BIGINT PRIMARY KEY,`c*` TEXT) ENGINE=WideTable WITH (wildcard_column='c*')"); err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TABLE `" + table + "`")
			dir := t.TempDir()
			os.WriteFile(filepath.Join(dir, "seg.csv"), []byte("1,hello\n2,world\n"), 0o600)
			os.WriteFile(filepath.Join(dir, "seg.csv.columns"), []byte("id\nc000001\n"), 0o600)
			p := Params{DSN: dsn, Table: table, Dir: dir, Entry: entry, BatchRows: 2, Workers: 1}
			if err := RunWithProgress(context.Background(), p, nil); err != nil {
				t.Fatal(err)
			}
			var got string
			if err := db.QueryRow("SELECT c000001 FROM `" + table + "` WHERE id=1").Scan(&got); err != nil || got != "hello" {
				t.Fatal(got, err)
			}
		})
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := Params{DSN: "u:p@tcp(127.0.0.1:1)/db", Table: "t", Dir: t.TempDir(), Workers: 1, BatchRows: 1, Entry: "upsert"}
	os.WriteFile(filepath.Join(p.Dir, "seg.csv"), []byte("1\n"), 0o600)
	os.WriteFile(filepath.Join(p.Dir, "seg.csv.columns"), []byte("id\n"), 0o600)
	if err := RunWithProgress(ctx, p, nil); err == nil {
		t.Fatal("canceled run succeeded")
	}
}
