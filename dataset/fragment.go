package dataset

import "sync/atomic"

// Fragment represents a data fragment (e.g., a single Parquet file).
// Inspection (metadata loading) is deferred until after partition
// pruning to avoid unnecessary I/O.
type Fragment struct {
	// Path is the filesystem path of the fragment.
	Path string
	// PartitionExpr describes the partition keys for this fragment
	// (e.g., a=5000/b=2).
	PartitionExpr *PartitionExpr
	// metadataLoaded tracks whether file metadata has been loaded.
	metadataLoaded int32
	// metadata is the file-level metadata (simulated).
	metadata interface{}
	// schema is the fragment's schema (simulated).
	schema interface{}
}

// NewFragment creates a new fragment with the given path and
// partition expression. Metadata is NOT loaded at construction
// time — it is deferred.
func NewFragment(path string, partitionExpr *PartitionExpr) *Fragment {
	return &Fragment{
		Path:           path,
		PartitionExpr:  partitionExpr,
		metadataLoaded: 0,
	}
}

// Inspect loads the fragment's metadata. This simulates the
// I/O cost of reading a Parquet footer. The caller should ensure
// partition pruning has already occurred before calling Inspect.
func (f *Fragment) Inspect() error {
	if atomic.LoadInt32(&f.metadataLoaded) != 0 {
		return nil // already loaded
	}
	// Simulate metadata loading (footer read).
	f.metadata = "metadata:" + f.Path
	f.schema = "schema:" + f.Path
	atomic.StoreInt32(&f.metadataLoaded, 1)
	return nil
}

// MetadataLoaded returns true if the fragment's metadata has been loaded.
func (f *Fragment) MetadataLoaded() bool {
	return atomic.LoadInt32(&f.metadataLoaded) != 0
}

// Metadata returns the loaded metadata, or nil if not yet loaded.
func (f *Fragment) Metadata() interface{} {
	return f.metadata
}
