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
