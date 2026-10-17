package check

import (
	"testing"

	"github.com/innerr/widetable-loader/internal/gen"
)

func TestValueComparison(t *testing.T) {
	cfg := gen.Config{}
	for _, row := range []struct {
		idx       int
		want, got string
		equal     bool
	}{
		{1, "7", "7", true}, {1, "7", "8", false},
		{3, "1.500000", "1.5", true}, {3, "1.5", "1.6", false},
		{4, "true", "false", false},
		{5, `{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{5, `{"a":1}`, `{"a":2}`, false},
	} {
		if equalValue(row.idx, cfg, row.want, row.got) != row.equal {
			t.Fatal(row)
		}
	}
}
