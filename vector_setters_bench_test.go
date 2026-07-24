package duckdb

import (
	"testing"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func BenchmarkSetChunkValueDispatch(b *testing.B) {
	logicalType := mapping.CreateLogicalType(TYPE_BIGINT)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	if err := chunk.initFromTypes([]mapping.LogicalType{logicalType}, true); err != nil {
		b.Fatal(err)
	}
	defer chunk.close()

	b.ReportAllocs()
	b.ResetTimer()
	// Vary the value so interface conversion cannot reuse boxed constants.
	for i := range b.N {
		if err := SetChunkValue(chunk, 0, 0, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetRowValueDispatch(b *testing.B) {
	logicalType := mapping.CreateLogicalType(TYPE_BIGINT)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	if err := chunk.initFromTypes([]mapping.LogicalType{logicalType}, true); err != nil {
		b.Fatal(err)
	}
	defer chunk.close()
	row := Row{chunk: &chunk}

	b.ReportAllocs()
	b.ResetTimer()
	// Vary the value so interface conversion cannot reuse boxed constants.
	for i := range b.N {
		if err := SetRowValue(row, 0, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}
