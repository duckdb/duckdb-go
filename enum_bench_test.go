package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// getEnumCGO is the original (pre-optimization) implementation, preserved
// here solely for benchmarking purposes. It creates and destroys a
// LogicalType via CGO on every cell read.
func (vec *vector) getEnumCGO(rowIdx mapping.IdxT) string {
	var idx mapping.IdxT
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = mapping.IdxT(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = mapping.IdxT(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = mapping.IdxT(getPrimitive[uint32](vec, rowIdx))
	case TYPE_UBIGINT:
		idx = mapping.IdxT(getPrimitive[uint64](vec, rowIdx))
	}

	logicalType := mapping.VectorGetColumnType(vec.vec)
	defer mapping.DestroyLogicalType(&logicalType)
	return mapping.EnumDictionaryValue(logicalType, idx)
}

func setupEnumBench(b *testing.B, rowCount int) (*sql.DB, *Connector) {
	b.Helper()
	c := newConnectorWrapper(b, ``, nil)
	db := sql.OpenDB(c)

	_, err := db.Exec(`CREATE TYPE bench_enum AS ENUM ('alpha', 'beta', 'gamma', 'delta', 'epsilon')`)
	require.NoError(b, err)
	_, err = db.Exec(`CREATE TABLE bench_enum_tbl (val bench_enum)`)
	require.NoError(b, err)
	_, err = db.Exec(fmt.Sprintf(`
		INSERT INTO bench_enum_tbl
		SELECT (ARRAY['alpha','beta','gamma','delta','epsilon'])[1 + (i %% 5)]
		FROM generate_series(0, %d) AS t(i)
	`, rowCount-1))
	require.NoError(b, err)

	return db, c
}

var benchmarkEnumSink string

// BenchmarkEnumGetValue compares the optimized enumDict path (new)
// against the original CGO-per-cell path (old).
//
// Both sub-benchmarks iterate the same fetched vectors,
// differing only in the getter function called per cell:
//   - Dict: vec.getEnum()    — single []string slice index lookup
//   - CGO:  vec.getEnumCGO() — VectorGetColumnType + EnumDictionaryValue + DestroyLogicalType
func BenchmarkEnumGetValue(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("Dict/N=%d", n), func(b *testing.B) {
			benchEnumVector(b, n, false)
		})
		b.Run(fmt.Sprintf("CGO/N=%d", n), func(b *testing.B) {
			benchEnumVector(b, n, true)
		})
	}
}

func benchEnumVector(b *testing.B, rowCount int, useCGO bool) {
	b.Helper()
	db, c := setupEnumBench(b, rowCount)
	defer closeDbWrapper(b, db)
	defer closeConnectorWrapper(b, c)

	mc, err := c.Connect(context.Background())
	require.NoError(b, err)
	conn := mc.(*Conn)
	defer func() {
		require.NoError(b, conn.Close())
	}()

	b.ResetTimer()

	for b.Loop() {
		stmt, e := conn.Prepare(`SELECT val FROM bench_enum_tbl`)
		require.NoError(b, e)
		s := stmt.(*Stmt)

		dkRows, e := s.QueryContext(context.Background(), nil)
		require.NoError(b, e)
		r := dkRows.(*rows)

		count := 0

		for {
			duckChunk := mapping.FetchChunk(r.res)
			if duckChunk.Ptr == nil {
				break
			}

			var chunk DataChunk
			require.NoError(b, chunk.initFromDuckDataChunk(duckChunk, false))

			vec := &chunk.columns[0]
			if useCGO {
				for rowIdx := range chunk.size {
					benchmarkEnumSink = vec.getEnumCGO(mapping.IdxT(rowIdx))
				}
			} else {
				for rowIdx := range chunk.size {
					benchmarkEnumSink = vec.getEnum(mapping.IdxT(rowIdx))
				}
			}
			count += chunk.size
			chunk.close()
		}

		require.NoError(b, dkRows.Close())
		require.NoError(b, s.Close())
		require.Equal(b, rowCount, count)
	}

	b.StopTimer()
}
