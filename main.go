// widetable-loader: bulk data loading toolkit for wide tables.
// Subcommands: gen, split, load, check, bench.
package main

import (
	"fmt"
	"os"

	"github.com/innerr/widetable-loader/internal/gen"
	"github.com/innerr/widetable-loader/internal/load"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: widetable-loader <command> [flags]

commands:
  gen     generate synthetic CSV data, one file per key segment
  split   split an existing data file into key-disjoint segments
  load    load segment files concurrently (LOAD DATA or multi-row INSERT)
  check   verify row counts, checksums, index entries after loading
  bench   online read/write workload on loaded data`)
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
	case "split", "check", "bench":
		err = fmt.Errorf("%s: not implemented yet", os.Args[1])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
