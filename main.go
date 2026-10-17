// widetable-loader: bulk data loading toolkit for wide tables.
// Subcommands: gen, split, load, check, bench, daemon, dist.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/innerr/widetable-loader/internal/bench"
	"github.com/innerr/widetable-loader/internal/check"
	"github.com/innerr/widetable-loader/internal/daemon"
	"github.com/innerr/widetable-loader/internal/dist"
	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/load"
	"github.com/innerr/widetable-loader/internal/split"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: widetable-loader <command> [flags]

commands:
  gen     generate synthetic CSV data, one file per key segment
  split   split an existing data file into key-disjoint segments
  load    load segment files concurrently (LOAD DATA or multi-row INSERT)
  check   verify row counts, sampled values and index queries after loading
  bench   online read/write workload on loaded data
  daemon  per-node HTTP server: runs gen/load/bench jobs, reports host stats
  dist    controller: drive daemons on all nodes (gen/load/bench/stats/stop)`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "gen":
		err = gen.Run(os.Args[2:])
	case "load":
		err = load.Run(os.Args[2:])
	case "check":
		err = check.Run(os.Args[2:])
	case "daemon":
		err = daemon.Run(os.Args[2:])
	case "dist":
		err = dist.Run(os.Args[2:])
	case "bench":
		err = bench.Run(os.Args[2:])
	case "split":
		err = split.Run(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
