package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	benchmarkChunkRowsTotalRows = 32 * 2048
	queryNumericBenchmark       = `SELECT i, f FROM benchmark_rows ORDER BY i`
)

var benchmarkChunkRowsSink struct {
	sumInt   int64
	sumFloat float64
	sumLen   int
	rowCount int
}

func BenchmarkQueryContextRowsScan(b *testing.B) {
	db, conn, query := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var sumInt int64
		var sumFloat float64
		var sumLen int

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryContext(ctx, query, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			values := make([]driver.Value, 3)
			for {
				err := rows.Next(values)
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				sumInt += values[0].(int64)
				sumFloat += values[1].(float64)
				sumLen += len(values[2].(string))
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.sumInt = sumInt
		benchmarkChunkRowsSink.sumFloat = sumFloat
		benchmarkChunkRowsSink.sumLen = sumLen
	}
}

func BenchmarkQueryContextRowsCount(b *testing.B) {
	db, conn, query := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		rowCount := 0

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryContext(ctx, query, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			values := make([]driver.Value, 3)
			for {
				err := rows.Next(values)
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
				rowCount++
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.rowCount = rowCount
	}
}

func BenchmarkQueryChunksContextScan(b *testing.B) {
	db, conn, query := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var sumInt int64
		var sumFloat float64
		var sumLen int

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryChunksContext(ctx, query, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			for {
				chunk, err := rows.NextChunk()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
					intVal, err := chunk.GetValue(0, rowIdx)
					require.NoError(b, err)
					floatVal, err := chunk.GetValue(1, rowIdx)
					require.NoError(b, err)
					stringVal, err := chunk.GetValue(2, rowIdx)
					require.NoError(b, err)

					sumInt += intVal.(int64)
					sumFloat += floatVal.(float64)
					sumLen += len(stringVal.(string))
				}
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.sumInt = sumInt
		benchmarkChunkRowsSink.sumFloat = sumFloat
		benchmarkChunkRowsSink.sumLen = sumLen
	}
}

func BenchmarkQueryChunksContextGetValueNumericScan(b *testing.B) {
	db, conn, _ := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var sumInt int64
		var sumFloat float64

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryChunksContext(ctx, queryNumericBenchmark, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			for {
				chunk, err := rows.NextChunk()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
					intVal, err := chunk.GetValue(0, rowIdx)
					require.NoError(b, err)
					floatVal, err := chunk.GetValue(1, rowIdx)
					require.NoError(b, err)

					sumInt += intVal.(int64)
					sumFloat += floatVal.(float64)
				}
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.sumInt = sumInt
		benchmarkChunkRowsSink.sumFloat = sumFloat
	}
}

func BenchmarkQueryChunksContextTypedNumericScan(b *testing.B) {
	db, conn, _ := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var sumInt int64
		var sumFloat float64

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryChunksContext(ctx, queryNumericBenchmark, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			for {
				chunk, err := rows.NextChunk()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				ints, err := chunk.Int64Slice(0)
				require.NoError(b, err)
				floats, err := chunk.Float64Slice(1)
				require.NoError(b, err)

				for rowIdx := range ints {
					isNull, err := chunk.IsNull(0, rowIdx)
					if err != nil {
						return err
					}
					if !isNull {
						sumInt += ints[rowIdx]
					}

					isNull, err = chunk.IsNull(1, rowIdx)
					if err != nil {
						return err
					}
					if !isNull {
						sumFloat += floats[rowIdx]
					}
				}
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.sumInt = sumInt
		benchmarkChunkRowsSink.sumFloat = sumFloat
	}
}

func BenchmarkQueryChunksContextTypedStringScan(b *testing.B) {
	db, conn, query := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var sumInt int64
		var sumFloat float64
		var sumLen int

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryChunksContext(ctx, query, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			for {
				chunk, err := rows.NextChunk()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				ints, err := chunk.Int64Slice(0)
				require.NoError(b, err)
				floats, err := chunk.Float64Slice(1)
				require.NoError(b, err)
				strings, err := chunk.StringRefs(2)
				require.NoError(b, err)

				for rowIdx := range ints {
					isNull, err := chunk.IsNull(0, rowIdx)
					if err != nil {
						return err
					}
					if !isNull {
						sumInt += ints[rowIdx]
					}

					isNull, err = chunk.IsNull(1, rowIdx)
					if err != nil {
						return err
					}
					if !isNull {
						sumFloat += floats[rowIdx]
					}

					isNull, err = chunk.IsNull(2, rowIdx)
					if err != nil {
						return err
					}
					if !isNull {
						sumLen += strings[rowIdx].Len()
					}
				}
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.sumInt = sumInt
		benchmarkChunkRowsSink.sumFloat = sumFloat
		benchmarkChunkRowsSink.sumLen = sumLen
	}
}

func BenchmarkQueryChunksContextCount(b *testing.B) {
	db, conn, query := prepareChunkRowsBenchmark(b)
	defer closeConnWrapper(b, conn)
	defer closeDbWrapper(b, db)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		rowCount := 0

		err := conn.Raw(func(driverConn any) error {
			duckConn := driverConn.(*Conn)

			rows, err := duckConn.QueryChunksContext(ctx, query, nil)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, rows.Close())
			}()

			for {
				chunk, err := rows.NextChunk()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}

				rowCount += chunk.GetSize()
			}
		})
		require.NoError(b, err)

		benchmarkChunkRowsSink.rowCount = rowCount
	}
}

func prepareChunkRowsBenchmark(b *testing.B) (*sql.DB, *sql.Conn, string) {
	b.Helper()

	db := openDbWrapper(b, `?access_mode=READ_WRITE`)
	setupQuery := fmt.Sprintf(`
CREATE TABLE benchmark_rows AS
SELECT
	i::BIGINT AS i,
	i::DOUBLE * 0.5 AS f,
	('value-' || i::VARCHAR) AS s
FROM range(%d) AS t(i)`,
		benchmarkChunkRowsTotalRows,
	)
	_, err := db.Exec(setupQuery)
	require.NoError(b, err)

	conn := openConnWrapper(b, db, context.Background())
	return db, conn, `SELECT i, f, s FROM benchmark_rows ORDER BY i`
}
