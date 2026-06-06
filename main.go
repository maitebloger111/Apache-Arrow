package main

import (
	"fmt"
	"runtime"
	"time"
)

func main() {
	const totalFragments = 10000

	// Build a dataset with 10,000 Hive-style partitions.
	fragments := make([]*Fragment, totalFragments)
	for i := 0; i < totalFragments; i++ {
		fragments[i] = &Fragment{
			Path: fmt.Sprintf("/data/a=%d/b=2/data.parquet", i),
			PartitionValues: map[string]interface{}{
				"a": i,
				"b": 2,
			},
		}
	}
	ds := &Dataset{Fragments: fragments}
	fmt.Printf("Created %s\n", ds)

	// Filter for a single partition: a == 5000 AND b == 2
	filters := []Expression{
		{Field: "a", Op: Eq, Value: 5000},
		{Field: "b", Op: Eq, Value: 2},
	}
	fmt.Printf("Filter: %s AND %s\n", filters[0], filters[1])

	// Sequential pruning (deferred metadata, no file I/O).
	start := time.Now()
	matched := ds.GetFragments(filters)
	seqDur := time.Since(start)
	fmt.Printf("Sequential prune: %d fragments matched in %v\n", len(matched), seqDur)

	// Parallel pruning with expression cache.
	workers := runtime.NumCPU()
	start = time.Now()
	matched = PruneParallel(ds.Fragments, filters, workers)
	parDur := time.Since(start)
	fmt.Printf("Parallel prune (%d workers): %d fragments matched in %v\n", workers, len(matched), parDur)

	// Verify metadata is only loaded for matched fragments.
	for _, f := range matched {
		meta, err := f.LoadMetadata()
		if err != nil {
			fmt.Printf("Error loading metadata for %s: %v\n", f.Path, err)
			continue
		}
		fmt.Printf("Loaded metadata for matched fragment: path=%s rows=%s\n", f.Path, meta["row_count"])
	}
}
