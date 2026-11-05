package bench

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/genkey"
)

func TestCancellationDoesNotCountAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var errs int64
	if err := workloadError(ctx, &errs, "read", errors.New("connection closed")); !errors.Is(err, context.Canceled) || errs != 0 {
		t.Fatal(err, errs)
	}
	if err := workloadError(context.Background(), &errs, "read", errors.New("missing row")); err == nil || errs != 1 {
		t.Fatal(err, errs)
	}
}

func TestInvalidReadRatio(t *testing.T) {
	p := Params{DSN: "u:p@tcp(localhost:3306)/db", Table: "items", Threads: 1, DurationS: 1,
		ReadRatio: math.NaN(), TotalRows: 2, Segments: 1, Scatter: "none", Fill: 1}
	if err := p.Validate(); err == nil {
		t.Fatal("NaN read ratio accepted")
	}
}

func TestExistingWriteStatements(t *testing.T) {
	declared := []string{largeJSONColumn1, largeJSONColumn2, "c000001", "c000002"}
	rng := rand.New(rand.NewSource(7))
	for row := 0; row < 20; row++ {
		id := uint64(row + 1)
		statements, err := buildWriteStatements("items", id, declared, map[string]bool{}, rng,
			gen.Config{RowWidth: 64}, "existing")
		if err != nil {
			t.Fatal(err)
		}
		if len(statements) < 2 || len(statements) > 3 {
			t.Fatalf("got %d statements, want 2-3", len(statements))
		}
		seen := make(map[string]bool, len(declared))
		for _, statement := range statements {
			if !strings.HasPrefix(statement.query, "UPSERT INTO ") {
				t.Fatalf("not an UPSERT: %s", statement.query)
			}
			if len(statement.args) != len(statement.columns)+1 || statement.args[0] != id {
				t.Fatalf("arguments do not match columns: %#v", statement)
			}
			for index, name := range statement.columns {
				if seen[name] {
					t.Fatalf("column %s appears in multiple statements for id %d", name, id)
				}
				seen[name] = true
				if name != largeJSONColumn1 && name != largeJSONColumn2 {
					continue
				}
				value, ok := statement.args[index+1].(string)
				if !ok || len(value) != largeJSONValueBytes || !json.Valid([]byte(value)) {
					t.Fatalf("bad large JSON value for %s: type %T, bytes %d", name,
						statement.args[index+1], len(value))
				}
			}
		}
		if len(seen) != len(declared) {
			t.Fatalf("statements cover %d columns, want %d", len(seen), len(declared))
		}
		first := make(map[string]bool, len(statements[0].columns))
		for _, name := range statements[0].columns {
			first[name] = true
		}
		if !first[largeJSONColumn1] || !first[largeJSONColumn2] {
			t.Fatalf("first statement lacks large JSON columns: %v", statements[0].columns)
		}
	}
}

func TestExistingWriteModeValidation(t *testing.T) {
	p := Params{DSN: "u:p@tcp(localhost:3306)/db", Table: "items", Threads: 1, DurationS: 1,
		ReadRatio: 0, TotalRows: 4, Segments: 1, Scatter: "hash32", GapFactor: 1, Fill: 4,
		WriteMode: "existing", Columns: "xx_long_1_raw,xx_long_2_raw,c000001,c000002"}
	if err := p.Validate(); err != nil {
		t.Fatal("existing writes must not require gap keys:", err)
	}
	missingLarge := p
	missingLarge.Columns = "c000001,c000002"
	if err := missingLarge.Validate(); err == nil || !strings.Contains(err.Error(), "large JSON") {
		t.Fatal("missing large JSON columns accepted:", err)
	}
	invalidMode := p
	invalidMode.WriteMode = "bogus"
	if err := invalidMode.Validate(); err == nil || !strings.Contains(err.Error(), "unknown write-mode") {
		t.Fatal("invalid write mode accepted:", err)
	}
}

func TestSplitWritesOneCompleteRow(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE loader_split_write (id BIGINT PRIMARY KEY,c000001 INT,c000002 TEXT,c000003 DOUBLE,c000004 VARCHAR(8),c000005 TEXT) ENGINE=WideTable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TABLE loader_split_write")
	cfg := gen.Config{TotalRows: 4, Segments: 1, Scatter: "hash32", GapFactor: 2, Fields: 5, Fill: 5, RowWidth: 32}
	layout, _ := cfg.Layout(0)
	var errs int64
	numeric := map[string]bool{}
	rows, statements, err := splitWrite(context.Background(), db, "loader_split_write",
		cfg.ColumnNames()[1:], numeric, 4, 8, []genkey.Layout{layout},
		rand.New(rand.NewSource(7)), cfg, &errs)
	if err != nil {
		t.Fatal(err)
	}
	if rows < 2 || rows > 3 || statements < rows*2 {
		t.Fatal("one event must write 2-3 rows with 2-3 statements each:", rows, statements)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM loader_split_write WHERE c000001 IS NOT NULL AND c000002 IS NOT NULL AND c000003 IS NOT NULL AND c000004 IS NOT NULL AND c000005 IS NOT NULL").Scan(&count); err != nil || count != rows {
		t.Fatal("split write did not create the complete rows:", count, rows, err)
	}
	var final Report
	params := Params{DSN: dsn, Table: "loader_split_write", Threads: 1, DurationS: 1, ReadRatio: 0,
		TotalRows: 4, Segments: 1, Scatter: "hash32", GapFactor: 2, Fill: 5, RowWidth: 32}
	if err := RunWithReport(context.Background(), params, func(r Report) { final = r }); err != nil ||
		final.Writes == 0 || final.Errors != 0 {
		t.Fatal("normal completion:", err, final)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	params.DurationS = 60
	if err := RunWithReport(ctx, params, func(r Report) { final = r }); !errors.Is(err, context.DeadlineExceeded) || final.Errors != 0 {
		t.Fatal("active cancellation:", err, final)
	}
}
