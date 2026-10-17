package load

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMySQLCSVEncoding(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	input := filepath.Join(dir, "source.csv")
	file, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	records := [][]string{
		{"1", `\N`, "", "slash\\quote\"'\x00\n\r\x1a", "comma,value"},
		{"2", "text", strings.Repeat("x", 128*1024), "", "end"},
	}
	if err := writer.WriteAll(records); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	converted, err := mysqlCSV(input, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(converted)
	got, err := os.ReadFile(converted)
	if err != nil {
		t.Fatal(err)
	}
	want := "\"1\",\\N,\"\",\"slash\\\\quote\"\"'\\0\\n\\r\\Z\",\"comma,value\"\n" +
		"\"2\",\"text\",\"" + strings.Repeat("x", 128*1024) + "\",\"\",\"end\"\n"
	if string(got) != want {
		t.Fatalf("encoded CSV differs: got %d bytes, want %d", len(got), len(want))
	}
}

func TestMySQLCSVFailedConversionRemovesOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	input := filepath.Join(dir, "source.csv")
	if err := os.WriteFile(input, []byte("1,valid\n2,too,many\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := mysqlCSV(input, 2); err == nil || output != "" {
		t.Fatalf("invalid record accepted: output=%q error=%v", output, err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "loader-mysql-*.csv"))
	if err != nil || len(files) != 0 {
		t.Fatalf("failed conversion left files: %v, error=%v", files, err)
	}
}

func BenchmarkMySQLCSV(b *testing.B) {
	input := os.Getenv("LOADER_BENCH_CSV")
	if input == "" {
		b.Skip("LOADER_BENCH_CSV not set")
	}
	file, err := os.Open(input)
	if err != nil {
		b.Fatal(err)
	}
	record, err := csv.NewReader(file).Read()
	if err != nil {
		b.Fatal(err)
	}
	info, err := file.Stat()
	file.Close()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		output, err := mysqlCSV(input, len(record))
		if err != nil {
			b.Fatal(err)
		}
		if err := os.Remove(output); err != nil {
			b.Fatal(err)
		}
	}
}
