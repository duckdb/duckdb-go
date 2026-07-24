package duckdb

import (
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func benchmarkSetChunkValueType[T any](b *testing.B, typ Type, values []T) {
	b.Helper()
	logicalType := mapping.CreateLogicalType(typ)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	if err := chunk.initFromTypes([]mapping.LogicalType{logicalType}, true); err != nil {
		b.Fatal(err)
	}
	defer chunk.close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := SetChunkValue(chunk, 0, 0, values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetChunkValueDispatchTypes(b *testing.B) {
	b.Run("BIGINT_from_int", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_BIGINT, []int{1_000_000, 2_000_000})
	})
	b.Run("VARCHAR_from_string", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_VARCHAR, []string{"one", "two"})
	})
	b.Run("TIMESTAMP_from_time", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_TIMESTAMP, []time.Time{
			time.Unix(1, 0),
			time.Unix(2, 0),
		})
	})
	b.Run("INTERVAL", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_INTERVAL, []Interval{
			{Days: 1},
			{Days: 2},
		})
	})
	b.Run("UUID", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_UUID, []UUID{
			{1},
			{2},
		})
	})
}
