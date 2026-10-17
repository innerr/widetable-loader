// Package split partitions CSV rows by primary key and rejects duplicates.
package split

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func Run(args []string) error {
	fs := flag.NewFlagSet("split", flag.ContinueOnError)
	input := fs.String("in", "", "input CSV (id is the first column)")
	columns := fs.String("columns", "", "column sidecar (default: <in>.columns)")
	out := fs.String("out-dir", "", "new output directory")
	segments := fs.Int("segments", 8, "number of output segments")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" || *out == "" || *segments < 1 {
		return errors.New("in, out-dir and positive segments are required")
	}
	if *columns == "" {
		*columns = *input + ".columns"
	}
	b, err := os.ReadFile(*columns)
	if err != nil {
		return err
	}
	names := strings.Fields(string(b))
	if len(names) == 0 || names[0] != "id" {
		return errors.New("first sidecar column must be id")
	}
	f, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = len(names)
	parent := filepath.Dir(*out)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".split-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	files := make([]*os.File, *segments)
	writers := make([]*csv.Writer, *segments)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for i := range files {
		path := filepath.Join(stage, fmt.Sprintf("seg-%d.csv", i))
		files[i], err = os.Create(path)
		if err != nil {
			return err
		}
		writers[i] = csv.NewWriter(files[i])
		if err := os.WriteFile(path+".columns", b, 0o644); err != nil {
			return err
		}
	}
	seen := map[int64]bool{}
	for {
		rec, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		id, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid signed BIGINT id %q: %w", rec[0], err)
		}
		if seen[id] {
			return fmt.Errorf("duplicate primary key %d", id)
		}
		seen[id] = true
		if err := writers[uint64(id)%uint64(*segments)].Write(rec); err != nil {
			return err
		}
	}
	for i, w := range writers {
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
		if err := files[i].Close(); err != nil {
			return err
		}
		files[i] = nil
	}
	if _, err := os.Stat(*out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("output directory already exists")
	}
	return os.Rename(stage, *out)
}
