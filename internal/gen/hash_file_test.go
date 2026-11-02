package gen

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestHashFileRoundTripAndFallback(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "seg-0.csv")
	content := []byte("a,b\n1,2\n")
	if err := os.WriteFile(seg, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seg+".columns", []byte("a\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Absent hash file: caller falls back to hashing the segment itself.
	got, err := ReadHashFile(seg)
	if err != nil || got != "" {
		t.Fatalf("absent hash file: got %q err %v", got, err)
	}

	// csv segment: hash is segment + column list file.
	if err := WriteHashFile(seg, true); err != nil {
		t.Fatal(err)
	}
	got, err = ReadHashFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := SegmentHash(seg, true)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip: got %q want %q", got, want)
	}
	segSum := sha256.Sum256(content)
	if got[:64] != hex.EncodeToString(segSum[:]) {
		t.Fatalf("hash file must start with the segment hash: %q", got[:64])
	}

	// sql segment: hash is the segment alone, no column list file.
	sqlSeg := filepath.Join(dir, "seg-0.sql")
	if err := os.WriteFile(sqlSeg, []byte("INSERT INTO t VALUES (1);\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteHashFile(sqlSeg, false); err != nil {
		t.Fatal(err)
	}
	gotSQL, err := ReadHashFile(sqlSeg)
	if err != nil {
		t.Fatal(err)
	}
	wantSQL, err := SegmentHash(sqlSeg, false)
	if err != nil {
		t.Fatal(err)
	}
	if gotSQL != wantSQL {
		t.Fatalf("sql round trip: got %q want %q", gotSQL, wantSQL)
	}
	if HashFilePath(seg) != seg+".hash" {
		t.Fatalf("unexpected hash file path: %s", HashFilePath(seg))
	}
}
