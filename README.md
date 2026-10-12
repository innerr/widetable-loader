# widetable-loader

Bulk data loading toolkit for wide tables: generate synthetic data, split
existing data into conflict-free segments, load segments concurrently, and
verify the result.

## Subcommands

| Command | Purpose |
|---|---|
| `gen` | Generate synthetic CSV data, one file per key segment |
| `split` | Split an existing data file into key-disjoint segments with duplicate-key detection |
| `load` | Load segment files concurrently (`LOAD DATA` entry, or multi-row `INSERT`) |
| `check` | Verify row counts, checksums, and index entries after loading |
| `bench` | Online read/write workload on loaded data (upsert events, point lookups) |

## Build

```sh
go build -o widetable-loader .
```

Prebuilt linux/amd64 binaries are attached to releases.

## Quickstart

```sh
# Generate 8 segments x 125k rows each (~1M rows total), blocked key layout
./widetable-loader gen --total-rows 1000000 --segments 8 --segment 0 --out seg0.csv
# ... repeat per segment (or run on each node for its own segments)

# Load them concurrently
./widetable-loader load --dsn "user:pass@tcp(host:port)/db" --table t1 --segments-dir . --workers 4

# Verify
./widetable-loader check --dsn "user:pass@tcp(host:port)/db" --table t1 --segments-dir .
```

## Bulk-load prerequisites

The fast SST path (`load --bulk`) assumes, and does **not** verify:

1. The target table is empty before the import task starts.
2. Primary keys are unique across the whole task (no overlap between segments
   and no overlap with existing rows).
3. No other writes happen against the table during the import.

If a batch fails before commit, roll it back and reload that segment. If the
commit result is uncertain (connection lost), verify whether the batch landed
before re-running it; do not blindly re-import.

## License

MIT
