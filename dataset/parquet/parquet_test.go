// Package parquet provides a Parquet dataset implementation.
package parquet

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/dataset"
	"github.com/stretchr/testify/assert"
)

func TestParquetDataset_OptimizeFragments(t *testing.T) {
	// Create a sample Parquet dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.Int64},
		{Name: "name", Type: arrow.String},
	}, nil)

	// Create a Parquet dataset
	parquetDataset, err := NewParquetDataset("path/to/dataset", schema)
	assert.NoError(t, err)

	// Create a filter expression
	filter := dataset.NewInExpression(
		arrow.NewField("id", arrow.Int64, nil),
		[]arrow.InterfaceData{int64(1), int64(2)},
	)

	// Optimize fragments
	optimizedFragments, err := parquetDataset.OptimizeFragments(context.Background(), filter)
	assert.NoError(t, err)

	// Verify optimized fragments
	assert.NotEmpty(t, optimizedFragments)
}
