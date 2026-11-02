package gen

// Customer pair mode: every row fills a random subset of catalog field pairs.
// A pair is two columns, a typed value column and a JSON raw column, named
// <family>_<field_id>_val and xx_<field_id>_raw. Field ids are 1..catalog and
// unique across families; each family owns catalog/5 consecutive ids and
// decides the value type. Because every row fills a different subset, the
// output format must be SQL statements (one INSERT per row) rather than a
// fixed-column CSV file.

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/innerr/widetable-loader/internal/sqlutil"
)

// PairRaw is one entry of the raw-column byte length distribution.
type PairRaw struct {
	Bytes  int
	Weight float64
}

// PairIndex fixes the cardinality of the customer index fields.
type PairIndex struct {
	DWDistinct         int
	RowsPerPgID        int
	RowsPerOssPath     int
	RowsPerAnonymousID int
	RowsPerAttachment  int
	RowsPerPackageName int
	RowsPerParentID    int
}

var pairFamilies = [5]string{"ms", "md", "mi", "mb", "ma"}

// pairSystemColumns are written on every row before the field pairs. Spatial
// columns (gP_Location, gP_TrajectoryPoints) and TTL are out of scope for the
// pair cases, so they are never generated.
var pairSystemColumns = []string{
	"dataType", "parentId", "parentDataType", "createTime", "updateTime", "dwId", "pgId",
}

// pairIndexColumns carry the customer's named indexes.
var pairIndexColumns = []string{
	"ms_dataUniqueId", "ms_ossPath", "ms_anonymousId", "ms_attachment",
	"ms_packageName", "mi_isDeleted",
}

type pairGen struct {
	perFamily  int
	index      PairIndex
	timeStart  int64
	timeSpan   int64
	rawDist    []PairRaw
	rawTotal   float64
	perm       []int
	buckets    [][]int // FillBuckets > 0: pre-picked column sets cycled per row
	bucketRows int     // PairBucketRows: consecutive rows per bucket before cycling
	rng        *rand.Rand
}

func newPairGen(cfg Config) *pairGen {
	total := 0.0
	for _, entry := range cfg.PairRawDist {
		total += entry.Weight
	}
	perm := make([]int, cfg.PairCatalog)
	for i := range perm {
		perm[i] = i
	}
	p := &pairGen{
		perFamily:  cfg.PairCatalog / len(pairFamilies),
		index:      cfg.PairIndex,
		timeStart:  cfg.PairTimeStart,
		timeSpan:   cfg.PairTimeSpan,
		rawDist:    cfg.PairRawDist,
		rawTotal:   total,
		perm:       perm,
		bucketRows: cfg.PairBucketRows,
		rng:        rand.New(rand.NewSource(int64(cfg.Seed*7919 + 17))),
	}
	if cfg.FillBuckets > 0 {
		p.buckets = make([][]int, cfg.FillBuckets)
		for b := range p.buckets {
			set := make([]int, cfg.PairFill)
			copy(set, p.pickRandom(cfg.PairFill))
			p.buckets[b] = set
		}
	}
	return p
}

// validatePairConfig rejects pair settings that cannot produce consistent rows.
func validatePairConfig(cfg Config) error {
	if cfg.PairCatalog <= 0 {
		return fmt.Errorf("pair-catalog must be positive in pair mode")
	}
	if cfg.PairCatalog%len(pairFamilies) != 0 {
		return fmt.Errorf("pair-catalog must be divisible by %d", len(pairFamilies))
	}
	if cfg.PairFill <= 0 || cfg.PairFill > cfg.PairCatalog {
		return fmt.Errorf("require 0 < pair-fill <= pair-catalog")
	}
	if len(cfg.PairRawDist) == 0 {
		return fmt.Errorf("pair-raw-bytes is required in pair mode")
	}
	total := 0.0
	for _, entry := range cfg.PairRawDist {
		if entry.Bytes <= 8 || entry.Weight <= 0 {
			return fmt.Errorf("pair-raw-bytes entries need bytes > 8 and weight > 0")
		}
		total += entry.Weight
	}
	if total <= 0 {
		return fmt.Errorf("pair-raw-bytes weights must sum above zero")
	}
	if cfg.PairTimeSpan <= 0 {
		return fmt.Errorf("pair-time-span must be positive")
	}
	for name, value := range map[string]int{
		"pair-dw-distinct":           cfg.PairIndex.DWDistinct,
		"pair-rows-per-pg-id":        cfg.PairIndex.RowsPerPgID,
		"pair-rows-per-oss-path":     cfg.PairIndex.RowsPerOssPath,
		"pair-rows-per-anonymous-id": cfg.PairIndex.RowsPerAnonymousID,
		"pair-rows-per-attachment":   cfg.PairIndex.RowsPerAttachment,
		"pair-rows-per-package-name": cfg.PairIndex.RowsPerPackageName,
		"pair-rows-per-parent-id":    cfg.PairIndex.RowsPerParentID,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive in pair mode", name)
		}
	}
	return nil
}

// pick returns the field ids filled on the given row: uniformly sampled
// without replacement from the whole catalog, or cycled from pre-picked
// buckets when FillBuckets is set (enables multi-row INSERT on the CSV path).
func (p *pairGen) pick(fill int, seq uint64) []int {
	if p.buckets != nil {
		step := seq
		if p.bucketRows > 1 {
			step /= uint64(p.bucketRows)
		}
		return p.buckets[int(step)%len(p.buckets)]
	}
	return p.pickRandom(fill)
}

func (p *pairGen) pickRandom(fill int) []int {
	for i := 0; i < fill; i++ {
		j := i + p.rng.Intn(len(p.perm)-i)
		p.perm[i], p.perm[j] = p.perm[j], p.perm[i]
	}
	return p.perm[:fill]
}

// rawBytes samples one target length from the raw distribution.
func (p *pairGen) rawBytes() int {
	if len(p.rawDist) == 1 {
		return p.rawDist[0].Bytes
	}
	point := p.rng.Float64() * p.rawTotal
	for _, entry := range p.rawDist {
		point -= entry.Weight
		if point < 0 {
			return entry.Bytes
		}
	}
	return p.rawDist[len(p.rawDist)-1].Bytes
}

func (p *pairGen) family(index int) string { return pairFamilies[index/p.perFamily] }

// value renders the typed value of one field. It returns the value as a JSON
// scalar (used inside the raw object) and as a SQL literal.
func (p *pairGen) value(family string) (string, string) {
	switch family {
	case "md":
		v := float64(p.rng.Intn(1000000)) / 100.0
		return strconv.FormatFloat(v, 'f', 2, 64), strconv.FormatFloat(v, 'f', 2, 64)
	case "mi":
		v := p.rng.Intn(1000000)
		return strconv.Itoa(v), strconv.Itoa(v)
	case "mb":
		if p.rng.Intn(2) == 0 {
			return "true", "true"
		}
		return "false", "false"
	case "ma":
		s := randString(24, p.rng)
		return quoteJSON(s), "'" + s + "'"
	default: // ms
		s := randString(18, p.rng)
		return quoteJSON(s), "'" + s + "'"
	}
}

// raw builds a valid JSON object holding the typed value plus filler, padded
// to the sampled byte length.
func (p *pairGen) raw(scalar string) string {
	head := `{"v":` + scalar + `,"p":"`
	tail := `"}`
	pad := p.rawBytes() - len(head) - len(tail)
	if pad < 0 {
		pad = 0
	}
	return head + randString(pad, p.rng) + tail
}

func quoteJSON(s string) string {
	encoded, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(encoded)
}

// systemValue renders the system or index column named name for sequence seq.
func (p *pairGen) systemValue(name string, seq uint64) string {
	switch name {
	case "dataType", "parentDataType":
		// pg 25%, fl 50%, cyberrecord 25%.
		switch seq % 4 {
		case 0:
			return "'pg'"
		case 3:
			return "'cyberrecord'"
		default:
			return "'fl'"
		}
	case "parentId":
		return strconv.FormatUint(seq/uint64(p.index.RowsPerParentID), 10)
	case "createTime", "updateTime":
		offset := int64(0)
		if p.timeSpan > 0 {
			offset = int64(seq % uint64(p.timeSpan))
		}
		return strconv.FormatInt(p.timeStart+offset, 10)
	case "dwId":
		return strconv.Itoa(int(seq % uint64(p.index.DWDistinct)))
	case "pgId":
		return strconv.FormatUint(seq/uint64(p.index.RowsPerPgID), 10)
	case "ms_dataUniqueId":
		return "'du-" + strconv.FormatUint(seq, 10) + "'"
	case "ms_ossPath":
		return "'oss/" + strconv.FormatUint(seq/uint64(p.index.RowsPerOssPath), 10) + "'"
	case "ms_anonymousId":
		return "'anon-" + strconv.FormatUint(seq/uint64(p.index.RowsPerAnonymousID), 10) + "'"
	case "ms_attachment":
		return "'att-" + strconv.FormatUint(seq/uint64(p.index.RowsPerAttachment), 10) + "'"
	case "ms_packageName":
		return "'pkg-" + strconv.FormatUint(seq/uint64(p.index.RowsPerPackageName), 10) + "'"
	case "mi_isDeleted":
		switch {
		case seq%100 == 0:
			return "1"
		case seq%100 == 50:
			return "2"
		default:
			return "0"
		}
	}
	return ""
}

// NextPairs renders one row as parallel column and value slices.
func (g *Generator) NextPairs(seq uint64) ([]string, []string, error) {
	if g.pair == nil {
		return nil, nil, fmt.Errorf("pair mode is not enabled for this generator")
	}
	if seq < g.row0 || seq >= g.row1 {
		return nil, nil, fmt.Errorf("seq %d outside segment rows [%d, %d)", seq, g.row0, g.row1)
	}
	p := g.pair
	cols := g.pairCols[:0]
	vals := g.pairVals[:0]
	cols = append(cols, "id")
	vals = append(vals, strconv.FormatUint(g.layout.ID(seq), 10))
	for _, name := range pairSystemColumns {
		cols = append(cols, name)
		vals = append(vals, p.systemValue(name, seq))
	}
	for _, name := range pairIndexColumns {
		cols = append(cols, name)
		vals = append(vals, p.systemValue(name, seq))
	}
	for _, index := range p.pick(g.cfg.PairFill, seq) {
		family := p.family(index)
		scalar, literal := p.value(family)
		id := index + 1
		cols = append(cols, fmt.Sprintf("%s_%06d_val", family, id),
			fmt.Sprintf("xx_%06d_raw", id))
		vals = append(vals, literal, "'"+p.raw(scalar)+"'")
	}
	g.pairCols = cols
	g.pairVals = vals
	return cols, vals, nil
}

// WriteSQLPairs writes one INSERT statement per row. Pair rows fill a
// different column subset on every row, so statements carry their own column
// list; the CSV path (WriteCSVPairsContext) instead uses the full column
// frame with \N holes.
func (g *Generator) WriteSQLPairs(w *bufio.Writer, table string) error {
	return g.WriteSQLPairsContext(context.Background(), w, table, 1)
}

// WriteSQLPairsMultiRow is WriteSQLPairs with multi-row grouping: adjacent
// rows filling the same columns are packed into one statement, up to multiRow
// rows. Building the statements here keeps the load phase to reading the file
// and replaying it. Pair rows share a column list only when the generator
// reuses one fixed set of filled columns (FillBuckets == 1); with more buckets
// every row keeps its own statement.
func (g *Generator) WriteSQLPairsMultiRow(w *bufio.Writer, table string, multiRow int) error {
	return g.WriteSQLPairsContext(context.Background(), w, table, multiRow)
}

// WriteSQLPairsContext is WriteSQLPairs with multi-row grouping and
// cancellation support for the daemon job path.
func (g *Generator) WriteSQLPairsContext(ctx context.Context, w *bufio.Writer, table string, multiRow int) error {
	if g.pair == nil {
		return fmt.Errorf("pair mode is not enabled for this generator")
	}
	if multiRow < 1 {
		multiRow = 1
	}
	head := "INSERT INTO " + sqlutil.QuoteIdentifier(table) + " ("
	var groupCols []string
	var groupVals [][]string
	flush := func() error {
		if len(groupVals) == 0 {
			return nil
		}
		quoted := make([]string, len(groupCols))
		for i, name := range groupCols {
			quoted[i] = sqlutil.QuoteIdentifier(name)
		}
		w.WriteString(head)
		w.WriteString(strings.Join(quoted, ","))
		w.WriteString(") VALUES ")
		for r, vals := range groupVals {
			if r > 0 {
				w.WriteString(",(")
			} else {
				w.WriteString("(")
			}
			w.WriteString(strings.Join(vals, ","))
			w.WriteString(")")
		}
		w.WriteString(";\n")
		groupVals = groupVals[:0]
		return nil
	}
	if err := g.forEachSeq(ctx, func(seq uint64) error {
		cols, vals, err := g.NextPairs(seq)
		if err != nil {
			return err
		}
		if len(groupVals) > 0 && (!sameColumns(groupCols, cols) || len(groupVals) == multiRow) {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(groupVals) == 0 {
			groupCols = append(groupCols[:0], cols...)
		}
		groupVals = append(groupVals, append([]string(nil), vals...))
		return nil
	}); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return w.Flush()
}

// sameColumns reports whether two rows fill exactly the same columns in the
// same order -- the condition for sharing one INSERT column list.
func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pairColumnIndex maps every full-frame column name to its position. The
// frame is id + system + index columns + both columns of every catalog pair,
// in catalog id order.
func pairColumnFrame(catalog int) []string {
	names := make([]string, 0, 1+len(pairSystemColumns)+len(pairIndexColumns)+2*catalog)
	names = append(names, "id")
	names = append(names, pairSystemColumns...)
	names = append(names, pairIndexColumns...)
	perFamily := catalog / len(pairFamilies)
	for index := 0; index < catalog; index++ {
		family := pairFamilies[index/perFamily]
		id := index + 1
		names = append(names, fmt.Sprintf("%s_%06d_val", family, id),
			fmt.Sprintf("xx_%06d_raw", id))
	}
	return names
}

// PairColumnNames returns the CSV header order for pair mode.
func PairColumnNames(cfg Config) []string {
	return pairColumnFrame(cfg.PairCatalog)
}

// NextPairsRecord renders one row in the full CSV column frame: unfilled
// columns stay empty (written as \N by the CSV writer).
func (g *Generator) NextPairsRecord(seq uint64) ([]string, error) {
	if g.pair == nil {
		return nil, fmt.Errorf("pair mode is not enabled for this generator")
	}
	if seq < g.row0 || seq >= g.row1 {
		return nil, fmt.Errorf("seq %d outside segment rows [%d, %d)", seq, g.row0, g.row1)
	}
	if g.pairFrame == nil {
		g.pairFrame = pairColumnFrame(g.cfg.PairCatalog)
	}
	record := make([]string, len(g.pairFrame))
	pos := make(map[string]int, len(g.pairFrame))
	for i, name := range g.pairFrame {
		pos[name] = i
	}
	cols, vals, err := g.NextPairs(seq)
	if err != nil {
		return nil, err
	}
	for i, name := range cols {
		v := vals[i]
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
			v = strings.ReplaceAll(v, "''", "'")
		}
		// Boolean literals are valid SQL keywords but fail strict parameter
		// conversion on the CSV path; store them as 0/1 instead.
		if strings.HasPrefix(name, "mb_") && v != "" {
			if v == "true" {
				v = "1"
			} else if v == "false" {
				v = "0"
			}
		}
		record[pos[name]] = v
	}
	return record, nil
}

// WriteCSVPairsContext writes every row in the full column frame; with
// FillBuckets set, consecutive rows share one column set and load-side
// multi-row INSERT can group them.
func (g *Generator) WriteCSVPairsContext(ctx context.Context, w *bufio.Writer) error {
	if g.pair == nil {
		return fmt.Errorf("pair mode is not enabled for this generator")
	}
	if err := g.forEachSeq(ctx, func(seq uint64) error {
		record, err := g.NextPairsRecord(seq)
		if err != nil {
			return err
		}
		writeRecord(w, record)
		return nil
	}); err != nil {
		return err
	}
	return w.Flush()
}

// RegisterPairFlags binds the pair-mode flags onto fs.
func RegisterPairFlags(fs *flag.FlagSet, cfg *Config) *string {
	fs.IntVar(&cfg.PairCatalog, "pair-catalog", 0,
		"pair mode: catalog field count (0 disables); each row fills a random subset")
	fs.IntVar(&cfg.PairFill, "pair-fill", 0, "pair mode: filled field pairs per row")
	spec := fs.String("pair-raw-bytes", "",
		`pair mode: raw column byte length distribution, e.g. "96:0.7,192:0.25,512:0.05"`)
	fs.Int64Var(&cfg.PairTimeStart, "pair-time-start", 0,
		"pair mode: createTime base in milliseconds")
	fs.Int64Var(&cfg.PairTimeSpan, "pair-time-span", 259200000,
		"pair mode: createTime span in milliseconds")
	fs.IntVar(&cfg.PairIndex.DWDistinct, "pair-dw-distinct", 100, "pair mode: distinct dwId values")
	fs.IntVar(&cfg.PairIndex.RowsPerPgID, "pair-rows-per-pg-id", 250, "pair mode: rows per pgId")
	fs.IntVar(&cfg.PairIndex.RowsPerOssPath, "pair-rows-per-oss-path", 4, "pair mode: rows per ossPath")
	fs.IntVar(&cfg.PairIndex.RowsPerAnonymousID, "pair-rows-per-anonymous-id", 1000,
		"pair mode: rows per anonymousId")
	fs.IntVar(&cfg.PairIndex.RowsPerAttachment, "pair-rows-per-attachment", 4,
		"pair mode: rows per attachment")
	fs.IntVar(&cfg.PairIndex.RowsPerPackageName, "pair-rows-per-package-name", 100,
		"pair mode: rows per packageName")
	fs.IntVar(&cfg.PairIndex.RowsPerParentID, "pair-rows-per-parent-id", 1000,
		"pair mode: rows per parentId")
	fs.IntVar(&cfg.PairBucketRows, "pair-bucket-rows", 0,
		"pair mode with fill-buckets: consecutive rows sharing one bucket's column set before cycling (0/1 = cycle every row; >1 enables CSV multi-row grouping)")
	return spec
}

// ParsePairRawDist parses the "bytes:weight,..." raw length distribution.
func ParsePairRawDist(spec string) ([]PairRaw, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	var out []PairRaw
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("bad pair-raw-bytes entry %q (want bytes:weight)", part)
		}
		size, err := strconv.Atoi(strings.TrimSpace(pair[0]))
		if err != nil {
			return nil, fmt.Errorf("bad pair-raw-bytes size in %q: %v", part, err)
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(pair[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("bad pair-raw-bytes weight in %q: %v", part, err)
		}
		out = append(out, PairRaw{Bytes: size, Weight: weight})
	}
	return out, nil
}
