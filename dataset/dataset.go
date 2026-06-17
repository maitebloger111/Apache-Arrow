package dataset

import (
	"context"
	"runtime"
	"sort"
	"sync"
)

// Dataset represents a collection of data fragments, organized
// with optional Hive-style partitioning.
type Dataset struct {
	// fragments is the full list of fragments in the dataset.
	fragments []*Fragment
	// partitionFields lists known partition columns (e.g., ["a", "b"]).
	partitionFields []string
	// simplifier caches expression simplifications.
	simplifier *ExpressionSimplifier
	// numWorkers controls parallelism for fragment filtering.
	numWorkers int
}

// DatasetOption configures dataset creation.
type DatasetOption func(*Dataset)

// WithNumWorkers sets the number of parallel workers for fragment filtering.
// Defaults to runtime.NumCPU().
func WithNumWorkers(n int) DatasetOption {
	return func(d *Dataset) {
		if n > 0 {
			d.numWorkers = n
		}
	}
}

// NewDataset creates a new dataset from a list of fragments.
func NewDataset(fragments []*Fragment, partitionFields []string, opts ...DatasetOption) *Dataset {
	d := &Dataset{
		fragments:       fragments,
		partitionFields: partitionFields,
		simplifier:      NewExpressionSimplifier(),
		numWorkers:      runtime.NumCPU(),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// FragmentCount returns the total number of fragments in the dataset.
func (d *Dataset) FragmentCount() int {
	return len(d.fragments)
}

// GetFragments returns the subset of fragments that match the given
// filter expression.
//
// Key optimizations:
//  1. Partition pruning occurs BEFORE metadata loading — fragments
//     eliminated by partition filtering never trigger I/O.
//  2. Fragment filtering is parallelized across workers using
//     goroutines with bounded concurrency.
//  3. Expression simplification results are cached to avoid
//     redundant AST traversals for fragments with identical
//     partition keys.
func (d *Dataset) GetFragments(ctx context.Context, filter *Expression) ([]*Fragment, error) {
	if filter == nil {
		// No filter: return all fragments but still defer metadata.
		return d.fragments, nil
	}

	totalFragments := len(d.fragments)
	if totalFragments == 0 {
		return nil, nil
	}

	// Phase 1: Parallel partition pruning.
	// We evaluate the filter against each fragment's partition
	// expression. This is cheap (no I/O) and eliminates irrelevant
	// fragments before any metadata inspection.
	matched := d.filterFragmentsParallel(ctx, filter, totalFragments)

	// Phase 2: (Deferred) metadata inspection.
	// Fragments that passed partition pruning may now have their
	// metadata loaded. For this optimization, we only inspect
	// fragments that are actually needed for scanning.
	// In a full implementation, this would happen lazily during
	// the actual scan phase.
	simplifiedCount := 0
	var simplifierMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, d.numWorkers)

	for _, f := range matched {
		wg.Add(1)
		sem <- struct{}{}
		go func(frag *Fragment) {
			defer wg.Done()
			defer func() { <-sem }()

			// Simplify the filter using the partition guarantee.
			// Cache hit avoids redundant AST traversal.
			_ = d.simplifier.SimplifyWithGuarantee(filter, frag.PartitionExpr)

			simplifierMu.Lock()
			simplifiedCount++
			simplifierMu.Unlock()
		}(f)
	}
	wg.Wait()

	_ = simplifiedCount
	return matched, nil
}

// filterFragmentsParallel evaluates the filter against each fragment's
// partition expression in parallel using bounded goroutines.
func (d *Dataset) filterFragmentsParallel(ctx context.Context, filter *Expression, total int) []*Fragment {
	// Pre-allocate result slice with estimated capacity.
	matched := make([]*Fragment, 0, total)

	type result struct {
		fragment *Fragment
		match    bool
	}

	results := make([]result, total)
	var mu sync.Mutex

	// Use bounded parallelism.
	workerCount := d.numWorkers
	if workerCount > total {
		workerCount = total
	}

	var wg sync.WaitGroup
	work := make(chan int, total)

	// Enqueue all fragment indices.
	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)

	// Launch workers.
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range work {
				select {
				case <-ctx.Done():
					mu.Lock()
					results[idx] = result{d.fragments[idx], false}
					mu.Unlock()
					continue
				default:
				}

				frag := d.fragments[idx]
				match := frag.PartitionExpr.Matches(filter)

				mu.Lock()
				results[idx] = result{frag, match}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Collect matched fragments in original order.
	for _, r := range results {
		if r.match {
			matched = append(matched, r.fragment)
		}
	}

	return matched
}

// filterFragmentsSequential evaluates the filter against each fragment
// sequentially. Used as a fallback or for comparison in benchmarks.
func (d *Dataset) filterFragmentsSequential(ctx context.Context, filter *Expression) []*Fragment {
	matched := make([]*Fragment, 0, len(d.fragments))
	for _, frag := range d.fragments {
		select {
		case <-ctx.Done():
			return matched
		default:
		}
		if frag.PartitionExpr.Matches(filter) {
			matched = append(matched, frag)
		}
	}
	return matched
}

// SimplifierStats returns cache hit/miss statistics.
func (d *Dataset) SimplifierStats() (hits, misses int64) {
	return d.simplifier.CacheHits(), d.simplifier.CacheMisses()
}

// PartitionFields returns the known partition columns.
func (d *Dataset) PartitionFields() []string {
	cp := make([]string, len(d.partitionFields))
	copy(cp, d.partitionFields)
	return cp
}

// ScanResult represents the result of a dataset scan.
type ScanResult struct {
	// Fragments is the list of fragments that passed filtering.
	Fragments []*Fragment
}

// Scanner scans a dataset with an optional filter expression.
// It orchestrates the full pipeline: partition pruning, metadata
// loading, and result collection.
type Scanner struct {
	dataset *Dataset
	filter  *Expression
}

// NewScanner creates a new scanner for the given dataset.
func NewScanner(dataset *Dataset) *Scanner {
	return &Scanner{dataset: dataset}
}

// WithFilter sets the filter expression for the scanner.
func (s *Scanner) WithFilter(filter *Expression) *Scanner {
	s.filter = filter
	return s
}

// Scan executes the scan pipeline:
//  1. Partition pruning (parallel, no I/O)
//  2. Expression simplification (cached)
//  3. Metadata loading deferred until after pruning
func (s *Scanner) Scan(ctx context.Context) (*ScanResult, error) {
	fragments, err := s.dataset.GetFragments(ctx, s.filter)
	if err != nil {
		return nil, err
	}
	return &ScanResult{Fragments: fragments}, nil
}

// SortFragments sorts fragments by path for deterministic output.
func SortFragments(fragments []*Fragment) {
	sort.Slice(fragments, func(i, j int) bool {
		return fragments[i].Path < fragments[j].Path
	})
}
