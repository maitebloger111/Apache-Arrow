package main

import (
	"fmt"
	"sync"
)

// Fragment represents a single data fragment in a partitioned dataset.
// Metadata loading is deferred until explicitly requested, so partition
// pruning can discard fragments without paying the I/O cost.
type Fragment struct {
	Path            string
	PartitionValues map[string]interface{}

	// metadata is loaded lazily — only after partition pruning passes.
	metadata     map[string]string
	metadataOnce sync.Once
	metadataErr  error
}

// LoadMetadata simulates loading expensive file-level metadata (e.g.
// Parquet footer). This is intentionally deferred so the pruning step
// can filter fragments without triggering I/O.
func (f *Fragment) LoadMetadata() (map[string]string, error) {
	f.metadataOnce.Do(func() {
		// Simulate reading file footer / schema metadata.
		f.metadata = map[string]string{
			"row_count": "1000",
			"format":    "parquet",
			"path":      f.Path,
		}
	})
	return f.metadata, f.metadataErr
}

// Dataset holds a collection of fragments from a partitioned dataset.
type Dataset struct {
	Fragments []*Fragment
}

// GetFragments returns fragments that match all the given filter
// expressions. Partition pruning is applied first — before any
// metadata is loaded — so irrelevant fragments never trigger I/O.
func (d *Dataset) GetFragments(filters []Expression) []*Fragment {
	if len(filters) == 0 {
		return d.Fragments
	}
	return pruneFragments(d.Fragments, filters)
}

// pruneFragments filters the fragment list using partition expressions.
// No file-level metadata is accessed during this step.
func pruneFragments(fragments []*Fragment, filters []Expression) []*Fragment {
	var result []*Fragment
	for _, f := range fragments {
		if matchesAll(f.PartitionValues, filters) {
			result = append(result, f)
		}
	}
	return result
}

// matchesAll checks whether a fragment's partition values satisfy every
// filter expression.
func matchesAll(partitionValues map[string]interface{}, filters []Expression) bool {
	for _, expr := range filters {
		if !expr.Evaluate(partitionValues) {
			return false
		}
	}
	return true
}

// String returns a human-readable summary of the dataset.
func (d *Dataset) String() string {
	return fmt.Sprintf("Dataset{fragments=%d}", len(d.Fragments))
}
