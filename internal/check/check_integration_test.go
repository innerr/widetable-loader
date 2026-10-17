package check

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/load"
)

func TestCheckLayoutsAndDetectCorruption(t *testing.T) {
	dsn := os.Getenv("LOADER_TEST_DSN")
	if dsn == "" {
		t.Skip("LOADER_TEST_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, layout := range []string{"hash32", "blocked", "none", "mul-hash"} {
		t.Run(layout, func(t *testing.T) {
			table := "loader_verify_" + layout
			if _, err := db.Exec("CREATE TABLE `" + table + "` (id BIGINT PRIMARY KEY,f1 BIGINT,f2 INT,c000001 INT,c000002 TEXT,c000003 DOUBLE,c000004 VARCHAR(8),c000005 TEXT, INDEX(c000001)) ENGINE=WideTable"); err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TABLE `" + table + "`")
			args := []string{"-total-rows", "8", "-segments", "2", "-scatter", layout, "-fields", "5",
				"-fixed-cols", "2", "-index-fields", "1", "-fill", "3", "-fill-buckets", "2", "-seed", "7", "-row-width", "32"}
			dir := t.TempDir()
			for segment, number := range []string{"0", "1"} {
				file := filepath.Join(dir, "seg-"+number+".csv")
				if err := gen.Run(append(append([]string{}, args...), "-segment", number, "-out", file)); err != nil {
					t.Fatal(segment, err)
				}
			}
			p := load.Params{DSN: dsn, Table: table, Dir: dir, Entry: "upsert", BatchRows: 2, Workers: 2}
			if err := load.RunWithProgress(context.Background(), p, nil); err != nil {
				t.Fatal(err)
			}
			checkArgs := append(append([]string{}, args...), "-dsn", dsn, "-table", table, "-sample-per-segment", "4")
			if err := Run(checkArgs); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("UPDATE `" + table + "` SET c000001=999"); err != nil {
				t.Fatal(err)
			}
			if err := Run(checkArgs); err == nil {
				t.Fatal("corrupted values accepted")
			}
		})
	}
}
