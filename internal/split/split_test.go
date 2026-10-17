package split

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSplitAndDuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.csv")
	os.WriteFile(in, []byte("1,a\n2,b\n3,c\n"), 0o600)
	os.WriteFile(in+".columns", []byte("id\nvalue\n"), 0o600)
	if err := Run([]string{"-in", in, "-out-dir", filepath.Join(dir, "segments"), "-segments", "2"}); err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(filepath.Join(dir, "segments", "seg-0.csv"))
	b, _ := os.ReadFile(filepath.Join(dir, "segments", "seg-1.csv"))
	if string(a) != "2,b\n" || string(b) != "1,a\n3,c\n" {
		t.Fatal(string(a), string(b))
	}
	os.WriteFile(in, []byte("1,a\n1,b\n"), 0o600)
	if err := Run([]string{"-in", in, "-out-dir", filepath.Join(dir, "duplicate")}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "duplicate")); !os.IsNotExist(err) {
		t.Fatal("partial output published")
	}
	os.WriteFile(in, []byte("-9223372036854775808,minimum\n-1,negative\n9223372036854775807,maximum\n"), 0o600)
	if err := Run([]string{"-in", in, "-out-dir", filepath.Join(dir, "signed-keys"), "-segments", "2"}); err != nil {
		t.Fatal("signed BIGINT keys rejected:", err)
	}
}
