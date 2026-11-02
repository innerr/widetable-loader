package gen

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
)

// HashFileSuffix names the file holding a segment's content hash. Generation
// writes it next to the segment so an import can read the hash instead of
// hashing the segment again: on a large dataset that pass reads hundreds of
// gigabytes per node before the first row is written.
const HashFileSuffix = ".hash"

// columnsSuffix is the file carrying a csv segment's column names.
const columnsSuffix = ".columns"

// HashFilePath is the hash file for the given segment file.
func HashFilePath(segment string) string { return segment + HashFileSuffix }

// WriteHashFile computes the combined hash a load would compute for this
// segment -- the segment content plus, for csv segments, the column list file
// -- and writes it next to the segment.
func WriteHashFile(segment string, includeColumns bool) error {
	sum, err := SegmentHash(segment, includeColumns)
	if err != nil {
		return err
	}
	return os.WriteFile(HashFilePath(segment), []byte(sum+"\n"), 0o644)
}

// SegmentHash is the hash a load computes for one segment.
func SegmentHash(segment string, includeColumns bool) (string, error) {
	sum, err := fileHash(segment)
	if err != nil {
		return "", err
	}
	if !includeColumns {
		return sum, nil
	}
	columnsHash, err := fileHash(segment + columnsSuffix)
	if err != nil {
		return "", err
	}
	return sum + columnsHash, nil
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ReadHashFile returns the hash stored by generation, or "" when the file is
// absent (a dataset generated with -no-hash-file, which exists only to
// reproduce rounds measured before hash files were written).
func ReadHashFile(segment string) (string, error) {
	b, err := os.ReadFile(HashFilePath(segment))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
