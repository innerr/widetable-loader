package gen

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
)

// HashSidecarSuffix names the sidecar file holding a segment's content hash.
// Generation writes it next to the segment so an import can read the hash
// instead of hashing the segment (and its column sidecar) again: on a large
// dataset that pass reads hundreds of gigabytes per node before the first row
// is written.
const HashSidecarSuffix = ".hash"

// HashSidecarPath is the sidecar file for the given segment file.
func HashSidecarPath(segment string) string { return segment + HashSidecarSuffix }

// WriteHashSidecar computes the combined hash a load would compute for this
// segment -- the segment content plus, for csv segments, the column sidecar --
// and writes it next to the segment.
func WriteHashSidecar(segment string, includeColumns bool) error {
	sum, err := SegmentHash(segment, includeColumns)
	if err != nil {
		return err
	}
	return os.WriteFile(HashSidecarPath(segment), []byte(sum+"\n"), 0o644)
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
	sidecar, err := fileHash(segment + ".columns")
	if err != nil {
		return "", err
	}
	return sum + sidecar, nil
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

// ReadHashSidecar returns the hash stored by generation, or "" when the
// sidecar is absent (a dataset generated before this existed).
func ReadHashSidecar(segment string) (string, error) {
	b, err := os.ReadFile(HashSidecarPath(segment))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
