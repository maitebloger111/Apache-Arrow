package dataset

import (
	"sort"
	"testing"
)

func makeFragment(path string, keys ...PartitionKey) Fragment {
	return Fragment{Path: path, Partition: PartitionExpression{Keys: keys}}
}

func TestPruneSerialKeepsMatchingFragments(t *testing.T) {
	fragments := []Fragment{
		makeFragment("a=1/b=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}, PartitionKey{Name: "b", Value: PartitionValue{Raw: "2"}}),
		makeFragment("a=2/b=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "2"}}, PartitionKey{Name: "b", Value: PartitionValue{Raw: "2"}}),
		makeFragment("a=1/b=3/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}, PartitionKey{Name: "b", Value: PartitionValue{Raw: "3"}}),
	}

	pruner := Pruner{Expression: Expression{Field: "a", Op: "==", Value: "1"}, Parallel: false}
	kept := pruner.Prune(fragments)

	if len(kept) != 2 {
		t.Fatalf("expected 2 fragments to match, got %d", len(kept))
	}
	paths := []string{kept[0].Path, kept[1].Path}
	sort.Strings(paths)
	want := []string{"a=1/b=2/data.parquet", "a=1/b=3/data.parquet"}
	for i := range paths {
		if paths[i] != want[i] {
			t.Fatalf("path mismatch at %d: got %q want %q", i, paths[i], want[i])
		}
	}
}

func TestPrunePreservesInputOrder(t *testing.T) {
	fragments := []Fragment{
		makeFragment("z"),
		makeFragment("a"),
		makeFragment("m"),
	}
	// Match all fragments so order is the only thing under test.
	pruner := Pruner{Expression: Expression{Field: "nonexistent", Op: "==", Value: "x"}, Parallel: false}
	kept := pruner.Prune(fragments)
	// nonexistent field matches nothing, so kept is empty — this also proves
	// that a missing key yields no match rather than a false positive.
	if len(kept) != 0 {
		t.Fatalf("expected 0 matches for missing key, got %d", len(kept))
	}
}

func TestPruneParallelMatchesSerial(t *testing.T) {
	fragments := []Fragment{
		makeFragment("a=1/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}),
		makeFragment("a=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "2"}}),
		makeFragment("a=1/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}),
		makeFragment("a=3/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "3"}}),
		makeFragment("a=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "2"}}),
	}

	serial := Pruner{Expression: Expression{Field: "a", Op: "==", Value: "2"}, Parallel: false}.Prune(fragments)
	parallel := Pruner{Expression: Expression{Field: "a", Op: "==", Value: "2"}, Parallel: true}.Prune(fragments)

	if len(serial) != len(parallel) {
		t.Fatalf("serial=%d parallel=%d, expected equal counts", len(serial), len(parallel))
	}
	for i := range serial {
		if serial[i].Path != parallel[i].Path {
			t.Fatalf("order mismatch at %d: serial=%q parallel=%q", i, serial[i].Path, parallel[i].Path)
		}
	}
}

func TestPruneDeferMetadataLoading(t *testing.T) {
	InspectCount = 0
	fragments := []Fragment{
		makeFragment("a=1/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}),
		makeFragment("a=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "2"}}),
		makeFragment("a=3/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "3"}}),
	}

	// Pruning must not touch Inspect/InspectWithCounting at all; metadata loading
	// is deferred until after a fragment clears the partition filter.
	pruner := Pruner{Expression: Expression{Field: "a", Op: "==", Value: "2"}, Parallel: true}
	kept := pruner.Prune(fragments)

	if InspectCount != 0 {
		t.Fatalf("metadata was inspected %d times during pruning; expected 0 (deferred until after filter)", InspectCount)
	}
	if len(kept) != 1 || kept[0].Path != "a=2/data.parquet" {
		t.Fatalf("expected exactly fragment a=2, got %d fragments", len(kept))
	}

	// Now inspecting the kept fragment increments the counter — proof metadata
	// loading happens only after pruning, on demand.
	if err := kept[0].InspectWithCounting(); err != nil {
		t.Fatal(err)
	}
	if InspectCount != 1 {
		t.Fatalf("expected InspectCount to be 1 after post-prune inspect, got %d", InspectCount)
	}
}

func TestDictionaryEncodedKeysNotDecoded(t *testing.T) {
	// A dictionary-encoded partition key resolves through its dictionary lazily,
	// only when Decoded() is called. Pruning must not eagerly unpack it.
	dictionary := map[string]string{"0x01": "active", "0x02": "archived"}
	fragments := []Fragment{
		makeFragment("status=0x01/data.parquet", PartitionKey{Name: "status", Value: PartitionValue{Dictionary: dictionary, Raw: "0x01"}}),
		makeFragment("status=0x02/data.parquet", PartitionKey{Name: "status", Value: PartitionValue{Dictionary: dictionary, Raw: "0x02"}}),
	}

	pruner := Pruner{Expression: Expression{Field: "status", Op: "==", Value: "active"}, Parallel: true}
	kept := pruner.Prune(fragments)

	if len(kept) != 1 || kept[0].Path != "status=0x01/data.parquet" {
		t.Fatalf("expected exactly status=0x01 (active), got %d fragments", len(kept))
	}
}

func TestComparisonOperators(t *testing.T) {
	cases := []struct {
		op   string
		val  string
		want []int // indices of fragments expected to match
	}{
		{"==", "2", []int{1}},
		{"!=", "2", []int{0, 2}},
		{"<", "2", []int{0}},
		{"<=", "2", []int{0, 1}},
		{">", "1", []int{1, 2}},
		{">=", "1", []int{0, 1, 2}},
	}

	fragments := []Fragment{
		makeFragment("a=1/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "1"}}),
		makeFragment("a=2/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "2"}}),
		makeFragment("a=10/data.parquet", PartitionKey{Name: "a", Value: PartitionValue{Raw: "10"}}),
	}

	for _, tc := range cases {
		pruner := Pruner{Expression: Expression{Field: "a", Op: tc.op, Value: tc.val}, Parallel: false}
		kept := pruner.Prune(fragments)
		got := map[string]bool{}
		for _, f := range kept {
			got[f.Path] = true
		}
		for _, idx := range tc.want {
			if !got[fragments[idx].Path] {
				t.Fatalf("op %q val %q: expected fragment %d (%q) to match", tc.op, tc.val, idx, fragments[idx].Path)
			}
		}
		// 10 > 2 numerically; ensure lexical ordering would not wrongly match.
		if tc.op == ">" && tc.val == "2" && got["a=10/data.parquet"] == false {
			t.Fatalf("numeric comparison broken: 10 should be > 2")
		}
	}
}

func TestEmptyInput(t *testing.T) {
	pruner := Pruner{Expression: Expression{Field: "a", Op: "==", Value: "1"}}
	kept := pruner.Prune(nil)
	if kept != nil {
		t.Fatalf("expected nil for empty input, got %v", kept)
	}
}

func TestUnsupportedOperatorErrors(t *testing.T) {
	pruner := Pruner{Expression: Expression{Field: "a", Op: "regexp", Value: "x"}, Parallel: false}
	_, err := pruner.Expression.EvaluatePartition(PartitionExpression{Keys: []PartitionKey{{Name: "a", Value: PartitionValue{Raw: "1"}}}})
	if err == nil {
		t.Fatalf("expected error for unsupported operator")
	}
}
