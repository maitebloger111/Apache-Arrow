package main

import (
	"testing"
)

func TestExpressionEvaluate(t *testing.T) {
	pv := map[string]interface{}{"a": 5000, "b": 2}

	tests := []struct {
		name   string
		expr   Expression
		expect bool
	}{
		{"eq match", Expression{"a", Eq, 5000}, true},
		{"eq no match", Expression{"a", Eq, 1}, false},
		{"neq match", Expression{"a", NotEq, 1}, true},
		{"lt match", Expression{"a", Lt, 6000}, true},
		{"lt no match", Expression{"a", Lt, 5000}, false},
		{"gt match", Expression{"a", Gt, 4000}, true},
		{"missing field", Expression{"c", Eq, 1}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.expr.Evaluate(pv)
			if got != tc.expect {
				t.Errorf("%s: got %v, want %v", tc.expr, got, tc.expect)
			}
		})
	}
}

func TestDeferredMetadataLoading(t *testing.T) {
	f := &Fragment{
		Path:            "/data/a=1/data.parquet",
		PartitionValues: map[string]interface{}{"a": 1},
	}

	// Metadata should be nil before loading.
	if f.metadata != nil {
		t.Fatal("metadata should be nil before LoadMetadata")
	}

	meta, err := f.LoadMetadata()
	if err != nil {
		t.Fatalf("LoadMetadata failed: %v", err)
	}
	if meta["path"] != f.Path {
		t.Errorf("expected path %s, got %s", f.Path, meta["path"])
	}
}

func TestPruneFragments(t *testing.T) {
	ds := buildTestDataset(100)
	filters := []Expression{
		{Field: "a", Op: Eq, Value: 42},
		{Field: "b", Op: Eq, Value: 2},
	}

	matched := ds.GetFragments(filters)
	if len(matched) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matched))
	}
	if matched[0].PartitionValues["a"] != 42 {
		t.Errorf("expected a=42, got a=%v", matched[0].PartitionValues["a"])
	}
}

func TestPruneNoFilters(t *testing.T) {
	ds := buildTestDataset(50)
	matched := ds.GetFragments(nil)
	if len(matched) != 50 {
		t.Fatalf("expected 50, got %d", len(matched))
	}
}

func TestPruneParallel(t *testing.T) {
	ds := buildTestDataset(10000)
	filters := []Expression{
		{Field: "a", Op: Eq, Value: 5000},
		{Field: "b", Op: Eq, Value: 2},
	}

	matched := PruneParallel(ds.Fragments, filters, 4)
	if len(matched) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matched))
	}
	if matched[0].PartitionValues["a"] != 5000 {
		t.Errorf("expected a=5000, got a=%v", matched[0].PartitionValues["a"])
	}
}

func TestPruneParallelNoMatch(t *testing.T) {
	ds := buildTestDataset(100)
	filters := []Expression{
		{Field: "a", Op: Eq, Value: 99999},
	}

	matched := PruneParallel(ds.Fragments, filters, 4)
	if len(matched) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(matched))
	}
}

func TestCacheHit(t *testing.T) {
	// Two fragments with the same partition values should both be
	// kept, and the cache should avoid duplicate evaluation.
	fragments := []*Fragment{
		{Path: "/data/a=1/part1.parquet", PartitionValues: map[string]interface{}{"a": 1}},
		{Path: "/data/a=1/part2.parquet", PartitionValues: map[string]interface{}{"a": 1}},
		{Path: "/data/a=2/part1.parquet", PartitionValues: map[string]interface{}{"a": 2}},
	}
	filters := []Expression{{Field: "a", Op: Eq, Value: 1}}

	matched := PruneParallel(fragments, filters, 2)
	if len(matched) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(matched))
	}
}

func TestMetadataNotLoadedForPruned(t *testing.T) {
	ds := buildTestDataset(100)
	filters := []Expression{
		{Field: "a", Op: Eq, Value: 50},
	}

	_ = ds.GetFragments(filters)

	// Pruned fragments should still have nil metadata.
	for _, f := range ds.Fragments {
		if f.PartitionValues["a"] != 50 && f.metadata != nil {
			t.Errorf("pruned fragment a=%v should not have metadata loaded", f.PartitionValues["a"])
		}
	}
}

func buildTestDataset(n int) *Dataset {
	fragments := make([]*Fragment, n)
	for i := 0; i < n; i++ {
		fragments[i] = &Fragment{
			Path: "/data/a=" + itoa(i) + "/b=2/data.parquet",
			PartitionValues: map[string]interface{}{
				"a": i,
				"b": 2,
			},
		}
	}
	return &Dataset{Fragments: fragments}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
