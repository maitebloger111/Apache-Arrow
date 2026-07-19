package main

import (
	"testing"
)

func TestBloomFilter_AddAndContains(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("hello")
	if !bf.MightContain("hello") {
		t.Fatal("expected to contain 'hello'")
	}
}

func TestBloomFilter_DefinitelyNotContains(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("exists")
	if bf.MightContain("definitely_not_added_xyz123") {
		// Could be false positive, but very unlikely with 1% FPR
		t.Log("false positive (expected ~1% rate) — not a failure")
	}
}

func TestPartitionPruner_ShouldRead(t *testing.T) {
	pp := NewPartitionPruner([]string{"date", "region"}, 1000)
	pp.AddPartitionValue("date", "2024-01-15")
	pp.AddPartitionValue("region", "us-east")

	if !pp.ShouldReadPartition("date", "2024-01-15") {
		t.Fatal("expected to read partition with matching value")
	}
	if pp.ShouldReadPartition("date", "1999-01-01") {
		t.Log("false positive possible — acceptable for Bloom filter")
	}
}

func TestPartitionPruner_PrunePartitions(t *testing.T) {
	pp := NewPartitionPruner([]string{"city"}, 100)
	pp.AddPartitionValue("city", "NYC")
	pp.AddPartitionValue("city", "LA")

	all := []string{"NYC", "LA", "Chicago", "Houston", "Phoenix"}
	kept := pp.PrunePartitions("city", all)

	// NYC and LA must be kept; others may or may not (false positives)
	foundNYC := false
	foundLA := false
	for _, v := range kept {
		if v == "NYC" {
			foundNYC = true
		}
		if v == "LA" {
			foundLA = true
		}
	}
	if !foundNYC || !foundLA {
		t.Fatal("expected NYC and LA to be kept after pruning")
	}
}

func TestPartitionPruner_UnknownColumn(t *testing.T) {
	pp := NewPartitionPruner([]string{"date"}, 100)
	// Unknown column should return true (must read — no filter)
	if !pp.ShouldReadPartition("unknown_col", "value") {
		t.Fatal("expected true for unknown column (no filter to skip)")
	}
}

func TestPartitionPruner_Concurrent(t *testing.T) {
	pp := NewPartitionPruner([]string{"key"}, 10000)
	done := make(chan bool)
	// Writer
	go func() {
		for i := 0; i < 1000; i++ {
			pp.AddPartitionValue("key", string(rune(i)))
		}
		done <- true
	}()
	// Reader
	go func() {
		for i := 0; i < 1000; i++ {
			pp.ShouldReadPartition("key", string(rune(i)))
		}
		done <- true
	}()
	<-done
	<-done
}

func TestBloomFilter_LargeCapacity(t *testing.T) {
	bf := NewBloomFilter(100000, 0.001) // 100K elements, 0.1% FPR
	for i := 0; i < 1000; i++ {
		bf.Add(string(rune(i)))
	}
	// All added elements should be found
	for i := 0; i < 1000; i++ {
		if !bf.MightContain(string(rune(i))) {
			t.Fatalf("expected to find element %d", i)
		}
	}
}
