package duckdb

import (
	"testing"
	"time"
)

func benchmarkSetChunkValueType[T any](b *testing.B, typ Type, values []T) {
	b.Helper()
	chunk := newTypeTestChunk(b, typ)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := SetChunkValue(chunk, 0, 0, values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetRowValueDispatch(b *testing.B) {
	chunk := newTypeTestChunk(b, TYPE_BIGINT)
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

func BenchmarkDataChunkSetValueDispatch(b *testing.B) {
	chunk := newTypeTestChunk(b, TYPE_BIGINT)
	values := []any{int64(1_000_000), int64(2_000_000)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := chunk.SetValue(0, 0, values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetChunkValueDispatchTypes(b *testing.B) {
	b.Run("BIGINT_from_int64", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_BIGINT, []int64{1_000_000, 2_000_000})
	})
	b.Run("BIGINT_from_int", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_BIGINT, []int{1_000_000, 2_000_000})
	})
	b.Run("VARCHAR_from_string", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_VARCHAR, []string{"one", "two"})
	})
	b.Run("VARCHAR_from_bytes", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_VARCHAR, [][]byte{[]byte("one"), []byte("two")})
	})
	b.Run("BLOB_from_string", func(b *testing.B) {
		benchmarkSetChunkValueType(b, TYPE_BLOB, []string{"one", "two"})
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
