package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/maitebloger111/Apache-Arrow/dataset"
)

// BenchmarkResult holds timing and stats from a benchmark run.
type BenchmarkResult struct {
	Name              string
	NumFragments      int
	MatchedFragments  int
	FilterTime        time.Duration
	MetadataInspected int
	SimplifierHits    int64
	SimplifierMisses  int64
}

func main() {
	fmt.Println("=== Apache Arrow — Optimized Partition Pruning Benchmark ===")

	// Run benchmarks for increasing dataset sizes.
	sizes := []int{100, 1000, 10000, 100000}

	for _, n := range sizes {
		result := benchmarkDataset(n)
		printResult(result)
		fmt.Println()
	}

	fmt.Println("=== All benchmarks completed successfully ===")
}

func benchmarkDataset(numFragments int) BenchmarkResult {
	// Build fragments simulating Hive-style partitioned data
	// with partition columns "a" and "b".
	fragments := make([]*dataset.Fragment, 0, numFragments)
	for i := 0; i < numFragments; i++ {
		path := fmt.Sprintf("/data/a=%d/b=2/data.parquet", i)
		pe := &dataset.PartitionExpr{
			Keys: []dataset.PartitionKey{
				{Field: "a", Value: i},
				{Field: "b", Value: 2},
			},
		}
		frag := dataset.NewFragment(path, pe)
		fragments = append(fragments, frag)
	}

	// Create dataset with parallel workers.
	ds := dataset.NewDataset(fragments, []string{"a", "b"},
		dataset.WithNumWorkers(runtime.NumCPU()))

	// Build a filter for a single partition: a == 5000 && b == 2.
	filter := &dataset.Expression{
		Op: dataset.OpAnd,
		Left: &dataset.Expression{
			Op:    dataset.OpEqual,
			Field: "a",
			Value: 5000,
		},
		Right: &dataset.Expression{
			Op:    dataset.OpEqual,
			Field: "b",
			Value: 2,
		},
	}

	// Run filtered scan.
	ctx := context.Background()
	start := time.Now()

	scanner := dataset.NewScanner(ds).WithFilter(filter)
	result, err := scanner.Scan(ctx)

	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return BenchmarkResult{}
	}

	// Count how many had metadata loaded (should only be the matched ones).
	metadataLoaded := 0
	for _, f := range result.Fragments {
		if f.MetadataLoaded() {
			metadataLoaded++
		}
	}

	hits, misses := ds.SimplifierStats()

	return BenchmarkResult{
		Name:              fmt.Sprintf("Hive-partitioned (a=int, b=2)"),
		NumFragments:      numFragments,
		MatchedFragments:  len(result.Fragments),
		FilterTime:        elapsed,
		MetadataInspected: metadataLoaded,
		SimplifierHits:    hits,
		SimplifierMisses:  misses,
	}
}

func printResult(r BenchmarkResult) {
	fmt.Printf("Dataset: %s\n", r.Name)
	fmt.Printf("  Total fragments : %d\n", r.NumFragments)
	fmt.Printf("  Matched         : %d\n", r.MatchedFragments)
	fmt.Printf("  Filter time     : %v\n", r.FilterTime)
	fmt.Printf("  Metadata loaded : %d (matched only — deferred)\n", r.MetadataInspected)
	fmt.Printf("  Cache hits      : %d\n", r.SimplifierHits)
	fmt.Printf("  Cache misses    : %d\n", r.SimplifierMisses)
}
