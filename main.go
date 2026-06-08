package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/dataset"
	"github.com/apache/arrow/go/arrow/dataset/parquet"
)

func main() {
	// Create a sample Parquet dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.Int64},
		{Name: "name", Type: arrow.String},
	}, nil)

	// Assume we have a Parquet dataset with the following structure:
	// - dataset/
	//   - id=1/
	//     - part-00000.snappy.parquet
	//   - id=2/
	//     - part-00000.snappy.parquet
	//   - ...

	// Create a dataset.Scanner with optimized partition pruning
	scanner, err := createOptimizedScanner("path/to/dataset", schema)
	if err != nil {
		log.Fatal(err)
	}

	// Scan the dataset
	for scanner.Scan(context.Background()) {
		record := scanner.Record()
		fmt.Println(record)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func createOptimizedScanner(datasetPath string, schema *arrow.Schema) (*dataset.Scanner, error) {
	// Create a Parquet dataset
	parquetDataset, err := parquet.NewParquetDataset(datasetPath, schema)
	if err != nil {
		return nil, err
	}

	// Create a filter expression
	filter := dataset.NewInExpression(
		arrow.NewField("id", arrow.Int64, nil),
		[]arrow.InterfaceData{int64(1), int64(2)},
	)

	// Optimize partition pruning by pushing down the filter expression
	optimizedFragments, err := parquetDataset.OptimizeFragments(context.Background(), filter)
	if err != nil {
		return nil, err
	}

	// Create a scanner with the optimized fragments
	scanner, err := dataset.NewScanner(
		optimizedFragments,
		nil,
		schema,
		dataset.ScannerOpts{},
	)
	if err != nil {
		return nil, err
	}

	return scanner, nil
}
