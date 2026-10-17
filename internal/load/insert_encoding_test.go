package load

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestInsertEncoderMatchesOriginal(t *testing.T) {
	cols := []string{"`id`", "`text`", "`number`"}
	records := [][]string{
		{"1", "", "3"}, {"2", "quotes'\\\n", "4"}, {"3", `\N`, "5"},
		{"4", `\N`, "6"}, {"5", "tail", `\N`}, {"6", "end", "7"},
	}
	for _, upsert := range []bool{false, true} {
		for _, limit := range []int{0, 1, 2, 50, 200} {
			encoder := insertEncoder{table: "rows", cols: cols, upsert: upsert}
			for pass := 0; pass < 2; pass++ {
				for start := 0; start < len(records); {
					names, wantArgs := filledRow(records[start], cols)
					wantRows := 1
					for wantRows < limit && start+wantRows < len(records) {
						other, values := filledRow(records[start+wantRows], cols)
						if !reflect.DeepEqual(names, other) {
							break
						}
						wantArgs = append(wantArgs, values...)
						wantRows++
					}
					gotSQL, gotArgs, gotRows := encoder.encode(records, start, limit)
					if gotSQL != insertSQL("rows", names, wantRows, upsert) ||
						!reflect.DeepEqual(gotArgs, wantArgs) || gotRows != wantRows {
						t.Fatalf("upsert=%v limit=%d start=%d: SQL=%s args=%v rows=%d",
							upsert, limit, start, gotSQL, gotArgs, gotRows)
					}
					start += gotRows
				}
			}
		}
	}
}

func TestInsertEncoderWideRows(t *testing.T) {
	cols := make([]string, 201)
	records := make([][]string, 207)
	for i := range cols {
		cols[i] = fmt.Sprintf("`c%06d`", i)
	}
	for row := range records {
		records[row] = make([]string, len(cols))
		for column := range cols {
			records[row][column] = fmt.Sprintf("%d:%d'\\\n", row, column)
		}
		if row >= 200 {
			records[row][1+row%2] = `\N`
		}
	}
	for _, upsert := range []bool{false, true} {
		encoder := insertEncoder{table: "import_rows", cols: cols, upsert: upsert}
		for _, limit := range []int{200, 50, 0, 200} {
			for start := 0; start < len(records); {
				names, wantArgs := filledRow(records[start], cols)
				wantRows := 1
				for wantRows < limit && start+wantRows < len(records) {
					other, values := filledRow(records[start+wantRows], cols)
					if !reflect.DeepEqual(names, other) {
						break
					}
					wantArgs = append(wantArgs, values...)
					wantRows++
				}
				stmt, args, rows := encoder.encode(records, start, limit)
				if stmt != insertSQL("import_rows", names, wantRows, upsert) ||
					!reflect.DeepEqual(args, wantArgs) || rows != wantRows {
					t.Fatalf("upsert=%v limit=%d start=%d: rows=%d, want %d",
						upsert, limit, start, rows, wantRows)
				}
				start += rows
			}
		}
	}
}

func BenchmarkInsertEncoding(b *testing.B) {
	for _, width := range []int{3, 201} {
		cols := make([]string, width)
		record := make([]string, width)
		for i := range cols {
			cols[i] = "`column" + strings.Repeat("x", i%5) + "`"
			record[i] = "value"
		}
		for _, upsert := range []bool{false, true} {
			name := "insert"
			if upsert {
				name = "upsert"
			}
			b.Run(fmt.Sprintf("%s/%d-cols/original", name, width), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					names, args := filledRow(record, cols)
					stmt := insertSQL("import_rows", names, 1, upsert)
					if len(args) != width || stmt == "" {
						b.Fatal("invalid encoding")
					}
				}
			})
			b.Run(fmt.Sprintf("%s/%d-cols/reused", name, width), func(b *testing.B) {
				encoder := insertEncoder{table: "import_rows", cols: cols, upsert: upsert}
				records := [][]string{record}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					stmt, args, rows := encoder.encode(records, 0, 0)
					if rows != 1 || len(args) != width || stmt == "" {
						b.Fatal("invalid encoding")
					}
				}
			})
		}
	}
}

func BenchmarkInsertSourceEncoding(b *testing.B) {
	path := os.Getenv("LOADER_BENCH_CSV")
	if path == "" {
		b.Skip("LOADER_BENCH_CSV not set")
	}
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	file.Close()
	if err != nil {
		b.Fatal(err)
	}
	columns, err := os.ReadFile(path + ".columns")
	if err != nil {
		b.Fatal(err)
	}
	cols := strings.Fields(string(columns))
	for i, col := range cols {
		cols[i] = quoteIdentifier(col)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, record := range records {
			names, args := filledRow(record, cols)
			if len(args) == 0 || insertSQL("import_rows", names, 1, false) == "" {
				b.Fatal("invalid encoding")
			}
		}
	}
}
