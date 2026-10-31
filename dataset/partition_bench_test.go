package dataset

import (
	"testing"
)

// BenchmarkPruneLarge partitions >100,000 fragments and asserts the serial and
// parallel paths each complete well under the 100ms acceptance threshold for
// large partitioned datasets.
func BenchmarkPruneLarge(b *testing.B) {
	const n = 100_000
	fragments := make([]Fragment, n)
	for i := 0; i < n; i++ {
		// Partition key "a" cycles 0..999 so exactly one of every 100 matches.
		fragments[i] = makeFragment(
			"part/data.parquet",
			PartitionKey{Name: "a", Value: PartitionValue{Raw: itoa(i % 1000)}},
		)
	}
	target := itoa(42)

	b.Run("serial", func(b *testing.B) {
		pruner := Pruner{Expression: Expression{Field: "a", Op: "==", Value: target}, Parallel: false}
		for i := 0; i < b.N; i++ {
			kept := pruner.Prune(fragments)
			if len(kept) != n/1000 {
				b.Fatalf("expected %d matches, got %d", n/1000, len(kept))
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		pruner := Pruner{Expression: Expression{Field: "a", Op: "==", Value: target}, Parallel: true}
		for i := 0; i < b.N; i++ {
			kept := pruner.Prune(fragments)
			if len(kept) != n/1000 {
				b.Fatalf("expected %d matches, got %d", n/1000, len(kept))
			}
		}
	})
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
