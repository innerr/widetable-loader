# widetable-loader

Bulk data loading toolkit for wide tables: generate synthetic data, split
existing data into key-disjoint segments, load segments concurrently, and
verify the result.

## Subcommands

| Command | Purpose |
|---|---|
| `gen` | Generate synthetic data, one file per key segment: CSV (default) or SQL statements |
| `split` | Split an existing data file into key-disjoint segments with duplicate-key detection |
| `load` | Transactional imports via `upsert` (default), `insert`, or `csv`; SQL segment files replay directly |
| `check` | Regenerate expected rows and compare counts, sampled values and index queries |
| `bench` | Online read/write workload on loaded data (upsert events, point lookups) |
| `daemon` | Per-node HTTP job server with cancellation and host statistics |
| `dist` | Control `gen`, `load`, `bench`, `stats`, and `stop` across node daemons |

## Build

```sh
CGO_ENABLED=0 go build -o widetable-loader .
```

Use a matching `GOOS` and `GOARCH` when cross-compiling.

## Quickstart

```sh
# Create this table in the target database first:
# CREATE TABLE items (
#   id BIGINT PRIMARY KEY, c000001 INT, c000002 TEXT, c000003 DOUBLE,
#   c000004 VARCHAR(8), c000005 TEXT, INDEX(c000001)
# ) ENGINE=WideTable;

mkdir -p segments
for segment in 0 1; do
  ./widetable-loader gen --total-rows 8 --segments 2 --segment "$segment" \
    --fields 5 --fixed-cols 0 --index-fields 1 --fill 3 --seed 7 \
    --out "segments/seg-$segment.csv"
done
./widetable-loader load --dsn "user:pass@tcp(host:port)/db" \
  --table items --segments-dir segments --workers 2
./widetable-loader check --dsn "user:pass@tcp(host:port)/db" \
  --table items --total-rows 8 --segments 2 --fields 5 \
  --fixed-cols 0 --index-fields 1 --fill 3 --seed 7
```

`gen` writes a `.columns` sidecar in file order. Empty generated fields use
`\N` for NULL; imported empty strings remain empty strings. `check` requires
the same generation parameters, including `scatter`, `file-cols`,
`fill-buckets`, `row-width`, and `seed`. It is not a checker for arbitrary
customer files.
Generation refuses to overwrite existing CSV or sidecar files; use a new
output path or remove the old pair explicitly.

Key layouts are `hash32` (default, at most 2^32 sequence values), `blocked`,
`none`, and `mul-hash`. All generated keys fit signed BIGINT. `total-rows`
must be positive and divisible by `segments`.

## Import and Resume

### SQL format

`gen -format sql -sql-table NAME` emits one INSERT statement per line
(`-sql-multi-row N` packs N rows per statement, `-sql-upsert` appends ON
DUPLICATE KEY UPDATE). With the same Config and seed, the SQL and CSV datasets
have identical primary keys row by row. A `segments_dir` containing `.sql`
files replays the statements directly: each line is sent as-is and the loader
counts a statement's actual rows (`),(``) toward `--batch-rows`, so
`--multi-row` is not needed in SQL mode. `.sql` segments cannot be mixed with
prepared/csv entries in one directory. Daemon `/api/gen` accepts the same
options via the `format`, `sql_table`, `sql_multi_row`, and `sql_upsert`
fields.

`--batch-rows` sets rows per INSERT transaction. `--multi-row` groups adjacent
rows with the same filled columns. `--prepared` caches statement templates;
it cannot be combined with `--multi-row`. Neither option applies to `csv`.
The CSV entry requires server-side `local_infile` support and rolls back
imports that produce warnings.

Each directory is locked during a load. `.load-progress.json` records the
target, completed files and SHA-256 hashes of the CSV and sidecar. Unfinished
segments are recorded before import and may contain committed batches.
Completed files are skipped; changed files or a different target are rejected.
Only non-bulk upsert automatically replays unfinished segments.

Progress does not inspect the database. After clearing, restoring or replacing
the target table, use `--no-resume` to import every file. For an unfinished
`insert`, `csv`, or bulk import, verify or clear the target before using it.
Connection loss during commit is uncertain, not evidence of rollback.

`split --in source.csv --out-dir segments --segments 4` uses the input sidecar
(or `--columns FILE`), rejects duplicate signed BIGINT keys and requires a new
output directory. Duplicate detection keeps all keys in memory; output opens
one file per segment.

## Distributed Jobs

Start `widetable-loader daemon --listen :9900` on each node. The daemon has no
authentication and accepts filesystem paths and database credentials; expose
it only on a trusted network. Job history is in memory and resets on restart.

Use `dist gen --nodes host1:9900:3306,host2:9900:3306` with the same generation
flags as `gen` and `--out-dir /path/on/every/node`. Segments are divided by
node count, with one active generator per node. `dist load` uses
`--dsn 'user:pass@tcp({host}:{port})/db'` and `--segments-dir` on every node.
`dist bench` uses the same node list and DSN template.

Failed or interrupted controllers cancel the jobs they started and report
cleanup failures. `dist stop --nodes ...` requests cancellation of all running
jobs; `--job ID` selects one ID. `dist stats --nodes ... --out stats.jsonl`
collects host samples. `bench` writes one complete row using 2-3 partial upserts
in a transaction; reads select existing primary keys. It is not a full-row or
secondary-index performance benchmark.

## Bulk-load prerequisites

The fast SST path (`load --bulk`) assumes, and does **not** verify:

1. The target table is empty before the import task starts.
2. Primary keys are unique across the whole task (no overlap between segments
   and no overlap with existing rows).
3. No other writes happen against the table during the import.

These assumptions also apply when `--entry upsert` is selected. Bulk mode is
not a way to update an existing table. Unordered input requires
`--allow-unsorted`; indexed targets require `--allow-sk`, subject to server
support. Not every server supports bulk mode with the CSV entry.

## Tests

```sh
go vet ./...
go test -race ./...
LOADER_TEST_DSN='user:pass@tcp(host:port)/testdb' go test -race -count=1 ./...
```

Integration tests create and remove their own WideTable tables in `testdb`.
Without `LOADER_TEST_DSN`, database tests are skipped.

## License

MIT
