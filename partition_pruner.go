// Package main provides an optimized partition pruner for large datasets.
//
// Issue: maitebloger111/Apache-Arrow#1
// Bounty: $10 (Opire)
//
// Implements a Bloom filter-based partition pruner that eliminates
// irrelevant partitions before reading Parquet files, dramatically
// reducing I/O for high-cardinality Hive-style partitioned datasets.
package main

import (
	"hash/fnv"
	"math"
	"sync"
)

// BloomFilter is a space-efficient probabilistic data structure
// used to test whether a partition key might contain matching values.
type BloomFilter struct {
	bits    []uint64
	numBits uint32
	numHash int
	mu      sync.RWMutex
}

// NewBloomFilter creates a Bloom filter with the given expected number
// of elements and false positive rate.
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := optimalNumBits(expectedElements, falsePositiveRate)
	k := optimalNumHash(m, expectedElements)
	return &BloomFilter{
		bits:    make([]uint64, (m+63)/64),
		numBits: m,
		numHash: k,
	}
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := 0; i < bf.numHash; i++ {
		idx := bf.hash(key, i)
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
}

// MightContain returns true if the key might be in the filter
// (false positive possible, false negative impossible).
func (bf *BloomFilter) MightContain(key string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for i := 0; i < bf.numHash; i++ {
		idx := bf.hash(key, i)
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(key string, seed int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	h.Write([]byte{byte(seed)})
	return h.Sum32() % bf.numBits
}

func optimalNumBits(n int, p float64) uint32 {
	return uint32(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
}

func optimalNumHash(m uint32, n int) int {
	return int(float64(m) / float64(n) * math.Ln2)
}

// PartitionPruner uses Bloom filters to skip irrelevant partitions.
type PartitionPruner struct {
	filters map[string]*BloomFilter // column name → Bloom filter
	mu      sync.RWMutex
}

// NewPartitionPruner creates a new pruner for the given partition columns.
func NewPartitionPruner(columns []string, expectedValues int) *PartitionPruner {
	pp := &PartitionPruner{
		filters: make(map[string]*BloomFilter),
	}
	for _, col := range columns {
		pp.filters[col] = NewBloomFilter(expectedValues, 0.01) // 1% FPR
	}
	return pp
}

// AddPartitionValue registers a value for a partition column.
func (pp *PartitionPruner) AddPartitionValue(column, value string) {
	pp.mu.RLock()
	bf, ok := pp.filters[column]
	pp.mu.RUnlock()
	if ok {
		bf.Add(value)
	}
}

// ShouldReadPartition returns true if the partition might contain
// matching data (based on Bloom filter probe), false if it can be skipped.
func (pp *PartitionPruner) ShouldReadPartition(column, value string) bool {
	pp.mu.RLock()
	bf, ok := pp.filters[column]
	pp.mu.RUnlock()
	if !ok {
		return true // No filter for this column — must read
	}
	return bf.MightContain(value)
}

// PrunePartitions filters a list of partition values, returning only
// those that might contain matching data.
func (pp *PartitionPruner) PrunePartitions(column string, values []string) []string {
	var kept []string
	for _, v := range values {
		if pp.ShouldReadPartition(column, v) {
			kept = append(kept, v)
		}
	}
	return kept
}
