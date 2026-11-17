package duckdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// BenchmarkMapScan measures conversion of a materialized DuckDB map, excluding
// query execution and vector initialization. Each operation scans one map.
func BenchmarkMapScan(b *testing.B) {
	for _, keyType := range []string{"BIGINT", "VARCHAR"} {
		for _, size := range []int{0, 1, 2, 8, 9, 10, 16, 17, 32, 33, 64, 100, 1_000, 10_000} {
			b.Run(fmt.Sprintf("%s/%d", keyType, size), func(b *testing.B) {
				c := newConnectorWrapper(b, ``, nil)
				defer closeConnectorWrapper(b, c)
				conn := openDriverConnWrapper(b, c).(*Conn)
				defer func() { require.NoError(b, conn.Close()) }()

				query := fmt.Sprintf(`SELECT MAP(
					list_transform(range(%d), lambda x: x::%s), range(%d))`, size, keyType, size)
				r, err := conn.QueryContext(context.Background(), query, nil)
				require.NoError(b, err)
				defer func() { require.NoError(b, r.Close()) }()

				var chunk DataChunk
				err = chunk.initFromDuckDataChunk(mapping.FetchChunk(r.(*rows).res), false)
				require.NoError(b, err)
				defer chunk.close()

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					value, err := chunk.GetValue(0, 0)
					if err != nil {
						b.Fatal(err)
					}
					m := value.(OrderedMap)
					if m.Len() != size {
						b.Fatalf("expected %d entries, got %d", size, m.Len())
					}
				}
			})
		}
	}
}

// BenchmarkMapWrite measures one map written to a reusable DataChunk or bound
// through a prepared statement. Input construction is outside the timed region.
func BenchmarkMapWrite(b *testing.B) {
	for _, path := range []string{"DataChunk", "Bind"} {
		for _, inputType := range []string{"OrderedMap", "Map"} {
			for _, size := range []int{1, 8, 1_000, 10_000} {
				b.Run(fmt.Sprintf("%s/%s/%d", path, inputType, size), func(b *testing.B) {
					var input any
					if inputType == "OrderedMap" {
						m := OrderedMap{keys: make([]any, size), values: make([]any, size)}
						for i := range size {
							m.keys[i], m.values[i] = int64(i), int64(i*3)
						}
						input = m
					} else {
						m := make(Map, size)
						for i := range size {
							m[int64(i)] = int64(i * 3)
						}
						input = m
					}

					var write func() error
					if path == "DataChunk" {
						info, err := NewMapInfo(mustTypeInfo(b, TYPE_BIGINT), mustTypeInfo(b, TYPE_BIGINT))
						require.NoError(b, err)
						chunk := newTestChunk(b, info.logicalType())
						write = func() error {
							chunk.reset(true)
							return chunk.SetValue(0, 0, input)
						}
					} else {
						db := openDbWrapper(b, ``)
						defer closeDbWrapper(b, db)
						stmt, err := db.Prepare(`SELECT ?::MAP(BIGINT, BIGINT)`)
						require.NoError(b, err)
						defer closePreparedWrapper(b, stmt)
						write = func() error {
							_, err := stmt.Exec(input)
							return err
						}
					}

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := write(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
