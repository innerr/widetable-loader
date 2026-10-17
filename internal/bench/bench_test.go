package bench

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"math/rand"
	"os"
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
	var writes, errs int64
	err = splitWrite(context.Background(), db, "loader_split_write", cfg.ColumnNames(), 4, 8,
		[]genkey.Layout{layout}, rand.New(rand.NewSource(7)), cfg, &writes, &errs)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM loader_split_write WHERE c000001 IS NOT NULL AND c000002 IS NOT NULL AND c000003 IS NOT NULL AND c000004 IS NOT NULL AND c000005 IS NOT NULL").Scan(&count); err != nil || count != 1 {
		t.Fatal("split write did not create one complete row:", count, err)
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
