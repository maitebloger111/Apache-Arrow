package main

import (
	"fmt"
	"strings"
	"sync"
)

// partitionKey builds a cache key from a fragment's partition values.
func partitionKey(pv map[string]interface{}) string {
	parts := make([]string, 0, len(pv))
	for k, v := range pv {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(parts, ",")
}

// PruneParallel filters fragments concurrently across multiple workers.
// It also caches expression results for fragments that share the same
// partition key values, avoiding redundant evaluations.
func PruneParallel(fragments []*Fragment, filters []Expression, workers int) []*Fragment {
	if len(filters) == 0 {
		return fragments
	}
	if workers < 1 {
		workers = 1
	}

	type result struct {
		index int
		keep  bool
	}

	// Cache for expression evaluation keyed by partition values.
	var mu sync.Mutex
	cache := make(map[string]bool)

	jobs := make(chan int, len(fragments))
	results := make(chan result, len(fragments))

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				f := fragments[idx]
				key := partitionKey(f.PartitionValues)

				mu.Lock()
				cached, found := cache[key]
				mu.Unlock()

				if found {
					results <- result{idx, cached}
					continue
				}

				keep := matchesAll(f.PartitionValues, filters)

				mu.Lock()
				cache[key] = keep
				mu.Unlock()

				results <- result{idx, keep}
			}
		}()
	}

	for i := range fragments {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	kept := make([]bool, len(fragments))
	for r := range results {
		kept[r.index] = r.keep
	}

	var out []*Fragment
	for i, f := range fragments {
		if kept[i] {
			out = append(out, f)
		}
	}
	return out
}
