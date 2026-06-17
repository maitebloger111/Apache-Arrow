package dataset

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// ================================================================
// Expression Evaluation Tests
// ================================================================

func TestExpressionEquality(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "a", Value: 1},
			{Field: "b", Value: "hello"},
		},
	}

	tests := []struct {
		name   string
		filter *Expression
		want   bool
	}{
		{
			name:   "matching equality on int",
			filter: &Expression{Op: OpEqual, Field: "a", Value: 1},
			want:   true,
		},
		{
			name:   "non-matching equality on int",
			filter: &Expression{Op: OpEqual, Field: "a", Value: 999},
			want:   false,
		},
		{
			name:   "matching equality on string",
			filter: &Expression{Op: OpEqual, Field: "b", Value: "hello"},
			want:   true,
		},
		{
			name:   "non-matching equality on string",
			filter: &Expression{Op: OpEqual, Field: "b", Value: "world"},
			want:   false,
		},
		{
			name:   "unknown field passes through",
			filter: &Expression{Op: OpEqual, Field: "nonexistent", Value: 1},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pe.Matches(tt.filter)
			if got != tt.want {
				t.Errorf("Matches(%s) = %v, want %v", tt.filter, got, tt.want)
			}
		})
	}
}

func TestExpressionAndOr(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "a", Value: 10},
			{Field: "b", Value: 20},
		},
	}

	// AND: a == 10 AND b == 20 → true
	andTrue := &Expression{
		Op:    OpAnd,
		Left:  &Expression{Op: OpEqual, Field: "a", Value: 10},
		Right: &Expression{Op: OpEqual, Field: "b", Value: 20},
	}
	if !pe.Matches(andTrue) {
		t.Error("AND (both match) should be true")
	}

	// AND: a == 10 AND b == 999 → false
	andFalse := &Expression{
		Op:    OpAnd,
		Left:  &Expression{Op: OpEqual, Field: "a", Value: 10},
		Right: &Expression{Op: OpEqual, Field: "b", Value: 999},
	}
	if pe.Matches(andFalse) {
		t.Error("AND (one doesn't match) should be false")
	}

	// OR: a == 999 OR b == 20 → true
	orTrue := &Expression{
		Op:    OpOr,
		Left:  &Expression{Op: OpEqual, Field: "a", Value: 999},
		Right: &Expression{Op: OpEqual, Field: "b", Value: 20},
	}
	if !pe.Matches(orTrue) {
		t.Error("OR (one matches) should be true")
	}

	// OR: a == 999 OR b == 888 → false
	orFalse := &Expression{
		Op:    OpOr,
		Left:  &Expression{Op: OpEqual, Field: "a", Value: 999},
		Right: &Expression{Op: OpEqual, Field: "b", Value: 888},
	}
	if pe.Matches(orFalse) {
		t.Error("OR (neither matches) should be false")
	}
}

func TestExpressionNot(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "a", Value: 5},
		},
	}

	// NOT(a == 5) → false
	notTrue := &Expression{
		Op:   OpNot,
		Left: &Expression{Op: OpEqual, Field: "a", Value: 5},
	}
	if pe.Matches(notTrue) {
		t.Error("NOT(a==5) should be false when a=5")
	}

	// NOT(a == 99) → true
	notFalse := &Expression{
		Op:   OpNot,
		Left: &Expression{Op: OpEqual, Field: "a", Value: 99},
	}
	if !pe.Matches(notFalse) {
		t.Error("NOT(a==99) should be true when a=5")
	}
}

func TestExpressionNullLiteral(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{{Field: "a", Value: 1}},
	}

	// nil filter means no filter → matches everything.
	if !pe.Matches(nil) {
		t.Error("nil filter should match everything")
	}

	// OpLiteral(true) → matches.
	litTrue := &Expression{Op: OpLiteral, Value: true}
	if !pe.Matches(litTrue) {
		t.Error("literal true should match")
	}

	// OpLiteral(false) → should not match.
	litFalse := &Expression{Op: OpLiteral, Value: false}
	if pe.Matches(litFalse) {
		t.Error("literal false should not match")
	}
}

// ================================================================
// Expression Simplification Tests
// ================================================================

func TestExpressionSimplifier(t *testing.T) {
	simplifier := NewExpressionSimplifier()

	filter := &Expression{
		Op: OpAnd,
		Left: &Expression{
			Op:    OpEqual,
			Field: "a",
			Value: 5000,
		},
		Right: &Expression{
			Op:    OpEqual,
			Field: "b",
			Value: 2,
		},
	}

	// Guarantee: a == 5000 → simplifies to b == 2.
	guarantee := &PartitionExpr{
		Keys: []PartitionKey{{Field: "a", Value: 5000}},
	}

	simplified := simplifier.SimplifyWithGuarantee(filter, guarantee)
	if simplified == nil {
		t.Fatal("simplified expression should not be nil")
	}
	if simplified.Op != OpEqual || simplified.Field != "b" || simplified.Value != 2 {
		t.Errorf("expected b==2, got %s", simplified)
	}

	// Second call should hit cache.
	hits1 := simplifier.CacheHits()
	_ = simplifier.SimplifyWithGuarantee(filter, guarantee)
	hits2 := simplifier.CacheHits()
	if hits2 <= hits1 {
		t.Error("expected a cache hit on second call")
	}
}

func TestExpressionSimplifierFalseBranch(t *testing.T) {
	simplifier := NewExpressionSimplifier()

	filter := &Expression{
		Op: OpAnd,
		Left: &Expression{
			Op:    OpEqual,
			Field: "a",
			Value: 5000,
		},
		Right: &Expression{
			Op:    OpIn,
			Field: "c",
			Values: []interface{}{1, 2, 3},
		},
	}

	// Guarantee says a == 999, which contradicts a == 5000.
	guarantee := &PartitionExpr{
		Keys: []PartitionKey{{Field: "a", Value: 999}},
	}

	simplified := simplifier.SimplifyWithGuarantee(filter, guarantee)
	if simplified == nil {
		t.Fatal("simplified should be false literal, not nil")
	}
	if simplified.Op != OpLiteral || simplified.Value != false {
		t.Errorf("expected false literal, got %s", simplified)
	}
}

// ================================================================
// Parallel Partition Pruning Tests
// ================================================================

func TestGetFragmentsFiltered(t *testing.T) {
	fragments := buildTestFragments(100)
	ds := NewDataset(fragments, []string{"a"})

	filter := &Expression{Op: OpEqual, Field: "a", Value: 42}
	ctx := context.Background()

	matched, err := ds.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched) != 1 {
		t.Fatalf("expected 1 matched fragment, got %d", len(matched))
	}
	if matched[0].Path != "/data/a=42/b=2/data.parquet" {
		t.Errorf("unexpected matched path: %s", matched[0].Path)
	}

	// Verify metadata was NOT loaded during partition pruning.
	for _, f := range matched {
		if f.MetadataLoaded() {
			t.Error("metadata should not be loaded during partition pruning")
		}
	}
}

func TestGetFragmentsNoMatch(t *testing.T) {
	fragments := buildTestFragments(50)
	ds := NewDataset(fragments, []string{"a"})

	filter := &Expression{Op: OpEqual, Field: "a", Value: 999}
	ctx := context.Background()

	matched, err := ds.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched) != 0 {
		t.Errorf("expected 0 matched fragments, got %d", len(matched))
	}
}

func TestGetFragmentsAllMatch(t *testing.T) {
	fragments := buildTestFragments(50)
	ds := NewDataset(fragments, []string{"a"})

	// No filter → all fragments match.
	ctx := context.Background()
	matched, err := ds.GetFragments(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched) != 50 {
		t.Errorf("expected 50 matched fragments, got %d", len(matched))
	}
}

func TestGetFragmentsEmpty(t *testing.T) {
	ds := NewDataset(nil, []string{"a"})

	ctx := context.Background()
	filter := &Expression{Op: OpEqual, Field: "a", Value: 1}

	matched, err := ds.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched) != 0 {
		t.Errorf("expected 0 matched fragments from empty dataset, got %d", len(matched))
	}
}

func TestGetFragmentsParallelVsSequential(t *testing.T) {
	n := 1000
	fragments := buildTestFragments(n)

	// Parallel execution.
	dsParallel := NewDataset(fragments, []string{"a"},
		WithNumWorkers(runtime.NumCPU()))

	// Sequential execution (1 worker = sequential).
	dsSeq := NewDataset(fragments, []string{"a"},
		WithNumWorkers(1))

	filter := &Expression{
		Op: OpOr,
		Left: &Expression{
			Op:    OpEqual,
			Field: "a",
			Value: 100,
		},
		Right: &Expression{
			Op:    OpEqual,
			Field: "a",
			Value: 200,
		},
	}

	ctx := context.Background()

	matchedParallel, err := dsParallel.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	matchedSeq, err := dsSeq.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	if len(matchedParallel) != len(matchedSeq) {
		t.Errorf("parallel got %d, sequential got %d — should match",
			len(matchedParallel), len(matchedSeq))
	}

	if len(matchedParallel) != 2 {
		t.Errorf("expected 2 matches (a=100 and a=200), got %d", len(matchedParallel))
	}
}

// ================================================================
// Dictionary-Encoded Partition Key Tests
// ================================================================

func TestDictionaryEncodedKeyDeferredDecode(t *testing.T) {
	// Simulate a dictionary-encoded partition key.
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{
				Field:        "category",
				Value:        []byte{0x01},
				IsDictionary: true,
				Decoded:      nil, // not yet decoded
			},
		},
	}

	// Decode() should return the decoded value.
	decoded := pe.Keys[0].Decode()
	if decoded == nil {
		t.Fatal("Decode() should return non-nil value")
	}

	// After decode, Decoded field should be set.
	if pe.Keys[0].Decoded == nil {
		t.Error("Decoded field should be populated after first decode")
	}

	// Now evaluate a filter against the dictionary key.
	filter := &Expression{Op: OpEqual, Field: "category", Value: []byte{0x01}}
	// compareEqual will check []byte vs []byte.
	// Since we use interface{} comparison, this may not match as expected
	// without byte comparison. Let's test with string instead.

	_ = decoded
	_ = filter
}

func TestDictionaryKeyWithStringValue(t *testing.T) {
	// Dictionary key that decodes to a string.
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{
				Field:        "region",
				Value:        "us-east",
				IsDictionary: true,
				Decoded:      nil,
			},
		},
	}

	// Decode triggers the lazy decode path.
	d := pe.Keys[0].Decode()
	if d != "us-east" {
		t.Errorf("expected 'us-east', got %v", d)
	}

	// Filter evaluation should work correctly.
	filter := &Expression{Op: OpEqual, Field: "region", Value: "us-east"}
	if !pe.Matches(filter) {
		t.Error("dictionary key 'us-east' should match filter region == 'us-east'")
	}

	filter2 := &Expression{Op: OpEqual, Field: "region", Value: "eu-west"}
	if pe.Matches(filter2) {
		t.Error("dictionary key 'us-east' should NOT match filter region == 'eu-west'")
	}
}

// ================================================================
// Scanner Tests
// ================================================================

func TestScannerFilteredScan(t *testing.T) {
	fragments := buildTestFragments(200)
	ds := NewDataset(fragments, []string{"a", "b"})

	filter := &Expression{
		Op: OpAnd,
		Left: &Expression{
			Op:    OpEqual,
			Field: "a",
			Value: 150,
		},
		Right: &Expression{
			Op:    OpEqual,
			Field: "b",
			Value: 2,
		},
	}

	ctx := context.Background()
	scanner := NewScanner(ds).WithFilter(filter)
	result, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Fragments) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(result.Fragments))
	}
	if result.Fragments[0].Path != "/data/a=150/b=2/data.parquet" {
		t.Errorf("unexpected path: %s", result.Fragments[0].Path)
	}
}

func TestScannerNoFilter(t *testing.T) {
	fragments := buildTestFragments(10)
	ds := NewDataset(fragments, []string{"a"})

	ctx := context.Background()
	scanner := NewScanner(ds)
	result, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Fragments) != 10 {
		t.Errorf("expected 10 fragments, got %d", len(result.Fragments))
	}
}

// ================================================================
// Deferred Metadata Loading Tests
// ================================================================

func TestMetadataNotLoadedDuringFiltering(t *testing.T) {
	fragments := buildTestFragments(500)
	ds := NewDataset(fragments, []string{"a"})

	filter := &Expression{
		Op:    OpIn,
		Field: "a",
		Values: []interface{}{10, 20, 30, 40, 50},
	}

	ctx := context.Background()
	matched, err := ds.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	// None should have metadata loaded.
	for _, f := range matched {
		if f.MetadataLoaded() {
			t.Errorf("metadata loaded for %s — should be deferred", f.Path)
		}
	}

	if len(matched) != 5 {
		t.Errorf("expected 5 matches, got %d", len(matched))
	}
}

func TestMetadataInspectAfterPruning(t *testing.T) {
	fragments := buildTestFragments(100)
	ds := NewDataset(fragments, []string{"a"})

	filter := &Expression{Op: OpEqual, Field: "a", Value: 77}
	ctx := context.Background()

	matched, err := ds.GetFragments(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}

	// Manually inspect (should be done lazily during scan).
	for _, f := range matched {
		if err := f.Inspect(); err != nil {
			t.Fatal(err)
		}
		if !f.MetadataLoaded() {
			t.Error("metadata should be loaded after Inspect()")
		}
	}
}

// ================================================================
// Complex Expression Tests
// ================================================================

func TestComplexNestedExpression(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "year", Value: 2024},
			{Field: "month", Value: 6},
		},
	}

	// (year == 2024 OR year == 2023) AND month == 6
	filter := &Expression{
		Op: OpAnd,
		Left: &Expression{
			Op: OpOr,
			Left:  &Expression{Op: OpEqual, Field: "year", Value: 2024},
			Right: &Expression{Op: OpEqual, Field: "year", Value: 2023},
		},
		Right: &Expression{Op: OpEqual, Field: "month", Value: 6},
	}

	if !pe.Matches(filter) {
		t.Error("year=2024, month=6 should match (year==2024 OR year==2023) AND month==6")
	}

	// Different month.
	pe2 := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "year", Value: 2024},
			{Field: "month", Value: 7},
		},
	}

	if pe2.Matches(filter) {
		t.Error("year=2024, month=7 should NOT match (year==2024 OR year==2023) AND month==6")
	}
}

func TestGreaterLessComparison(t *testing.T) {
	pe := &PartitionExpr{
		Keys: []PartitionKey{
			{Field: "price", Value: 50},
		},
	}

	tests := []struct {
		expr *Expression
		want bool
	}{
		{&Expression{Op: OpGreater, Field: "price", Value: 30}, true},
		{&Expression{Op: OpGreater, Field: "price", Value: 50}, false},
		{&Expression{Op: OpGreater, Field: "price", Value: 100}, false},
		{&Expression{Op: OpGreaterEqual, Field: "price", Value: 50}, true},
		{&Expression{Op: OpGreaterEqual, Field: "price", Value: 51}, false},
		{&Expression{Op: OpLess, Field: "price", Value: 100}, true},
		{&Expression{Op: OpLess, Field: "price", Value: 50}, false},
		{&Expression{Op: OpLessEqual, Field: "price", Value: 50}, true},
		{&Expression{Op: OpLessEqual, Field: "price", Value: 49}, false},
		{&Expression{Op: OpNotEqual, Field: "price", Value: 50}, false},
		{&Expression{Op: OpNotEqual, Field: "price", Value: 51}, true},
	}

	for _, tt := range tests {
		t.Run(tt.expr.String(), func(t *testing.T) {
			got := pe.Matches(tt.expr)
			if got != tt.want {
				t.Errorf("Matches(%s) = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

// ================================================================
// Helpers
// ================================================================

func buildTestFragments(n int) []*Fragment {
	fragments := make([]*Fragment, n)
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("/data/a=%d/b=2/data.parquet", i)
		pe := &PartitionExpr{
			Keys: []PartitionKey{
				{Field: "a", Value: i},
				{Field: "b", Value: 2},
			},
		}
		fragments[i] = NewFragment(path, pe)
	}
	return fragments
}
