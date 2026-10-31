package dataset

import (
	"fmt"
	"sync"
)

// PartitionValue is a single partition key value. It may be dictionary-encoded:
// when Dictionary is non-nil the raw bytes index into the dictionary rather than
// being decoded eagerly during expression evaluation. This keeps pruning from
// triggering premature decoding/unpacking of dictionary-encoded partition keys.
type PartitionValue struct {
	// Dictionary maps a raw key to its decoded value for dictionary-encoded partitions.
	Dictionary map[string]string
	// Raw is the on-disk/raw representation of the value.
	Raw string
}

// Decoded returns the decoded representation of the value. For dictionary-encoded
// partitions this resolves through the dictionary without allocating a new copy
// when one is not needed.
func (pv PartitionValue) Decoded() string {
	if pv.Dictionary != nil {
		if v, ok := pv.Dictionary[pv.Raw]; ok {
			return v
		}
	}
	return pv.Raw
}

// PartitionKey is a single partition key -> value binding.
type PartitionKey struct {
	Name  string
	Value PartitionValue
}

// PartitionExpression is the set of partition key/value pairs describing a
// fragment's location in a partitioned dataset (e.g. Hive-style a=1/b=2).
type PartitionExpression struct {
	Keys []PartitionKey
}

// ValueFor returns the decoded value for the named partition key, or the zero
// value and false when the key is not present.
func (pe PartitionExpression) ValueFor(name string) (string, bool) {
	for _, k := range pe.Keys {
		if k.Name == name {
			return k.Value.Decoded(), true
		}
	}
	return "", false
}

// Expression is a filter expression applied against partition expressions during
// fragment discovery. It is intentionally small and focused on the partition
// pruning use case: comparisons against a named partition key.
type Expression struct {
	// Field is the partition key the expression references.
	Field string
	// Op is the comparison operator ("==", "!=", "<", "<=", ">", ">=").
	Op string
	// Value is the comparison value.
	Value string
}

// EvaluatePartition tests whether the expression holds against a single partition
// expression. It is the hot path of fragment filtering and is kept free of any
// metadata/file-footer access.
func (e Expression) EvaluatePartition(pe PartitionExpression) (bool, error) {
	val, ok := pe.ValueFor(e.Field)
	if !ok {
		// Fragment does not constrain this partition key; it cannot match.
		return false, nil
	}
	cmp := compareStrings(val, e.Value)
	switch e.Op {
	case "==":
		return cmp == 0, nil
	case "!=":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unsupported operator %q", e.Op)
	}
}

// compareStrings returns an ordering of a vs b (-1, 0, +1). Numeric-looking
// values compare numerically so partition keys like a=9 vs a=10 order correctly.
func compareStrings(a, b string) int {
	ai, aOk := parseInt(a)
	bi, bOk := parseInt(b)
	if aOk && bOk {
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	}
	// Fall back to lexical comparison.
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func parseInt(s string) (int64, bool) {
	var n int64
	neg := false
	i := 0
	if i < len(s) && s[i] == '-' {
		neg = true
		i++
	}
	if i >= len(s) {
		return 0, false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int64(s[i]-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

// Fragment is a discoverable dataset fragment. It carries a partition expression
// but deliberately defers any file-level metadata/footer inspection until after
// the fragment clears the partition filter.
type Fragment struct {
	// Path is the fragment's on-disk location.
	Path string
	// Partition is the partition expression describing this fragment.
	Partition PartitionExpression
	// Inspected records whether file-level metadata has been loaded. Pruning must
	// not touch this field before filtering.
	Inspected bool
}

// Inspect simulates loading Parquet file footer metadata. It must only be called
// after a fragment has passed the partition filter.
func (f *Fragment) Inspect() error {
	f.Inspected = true
	return nil
}

// InspectCount tracks how many fragments have been inspected across a pruning run.
// It is exposed for tests to assert metadata loading is deferred until after pruning.
var InspectCount int

// InspectWithCounting is like Inspect but increments a package-level counter so
// tests can verify metadata loading is deferred until after pruning.
func (f *Fragment) InspectWithCounting() error {
	InspectCount++
	f.Inspected = true
	return nil
}

// Pruner applies a filter expression to a set of fragments, returning those that
// pass the partition filter. Pruning happens before any metadata inspection and
// can be parallelized across fragments.
type Pruner struct {
	// Expression is the filter expression to match against partition keys.
	Expression Expression
	// Parallel enables multi-threaded fragment filtering.
	Parallel bool
}

// pruneResult captures the outcome of filtering a single fragment.
type pruneResult struct {
	idx   int
	frag  Fragment
	match bool
	err   error
}

// Prune returns the fragments whose partition expressions satisfy the filter,
// preserving input order. When Parallel is true the evaluation is fanned out
// across goroutines (bounded by the number of fragments), which is the
// parallelized partition pruning path for large partitioned datasets.
func (p Pruner) Prune(fragments []Fragment) []Fragment {
	if len(fragments) == 0 {
		return nil
	}
	if !p.Parallel || len(fragments) < 2 {
		return p.pruneSerial(fragments)
	}
	return p.pruneParallel(fragments)
}

func (p Pruner) pruneSerial(fragments []Fragment) []Fragment {
	kept := make([]Fragment, 0, len(fragments))
	for i := range fragments {
		match, err := p.Expression.EvaluatePartition(fragments[i].Partition)
		if err != nil || !match {
			continue
		}
		kept = append(kept, fragments[i])
	}
	return kept
}

func (p Pruner) pruneParallel(fragments []Fragment) []Fragment {
	const workers = 8
	n := len(fragments)

	jobs := make(chan int, n)
	results := make(chan pruneResult, n)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				match, err := p.Expression.EvaluatePartition(fragments[i].Partition)
				results <- pruneResult{idx: i, frag: fragments[i], match: match, err: err}
			}
		}()
	}

	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect matches, then restore input order.
	matches := make([]pruneResult, 0, n)
	for r := range results {
		if r.err == nil && r.match {
			matches = append(matches, r)
		}
	}

	sortResultsByIdx(matches)
	kept := make([]Fragment, 0, len(matches))
	for _, r := range matches {
		kept = append(kept, r.frag)
	}
	return kept
}

func sortResultsByIdx(results []pruneResult) {
	// Insertion sort is fine for the small match sets typical of pruning.
	for i := 1; i < len(results); i++ {
		key := results[i]
		j := i - 1
		for j >= 0 && results[j].idx > key.idx {
			results[j+1] = results[j]
			j--
		}
		results[j+1] = key
	}
}
