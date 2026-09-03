package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

type vectorAccessBenchmarkMode uint8

const (
	vectorAccessBenchmarkRowExecutor vectorAccessBenchmarkMode = iota
	vectorAccessBenchmarkChunkRows
	vectorAccessBenchmarkVector
)

type vectorAccessBenchmarkUDF[T vectorValue] struct {
	info                TypeInfo
	mode                vectorAccessBenchmarkMode
	transform           func(T) T
	specialNullHandling bool
}

func (udf *vectorAccessBenchmarkUDF[T]) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos:      []TypeInfo{udf.info},
		ResultTypeInfo:      udf.info,
		SpecialNullHandling: udf.specialNullHandling,
	}
}

func (udf *vectorAccessBenchmarkUDF[T]) Executor() ScalarFuncExecutor {
	switch udf.mode {
	case vectorAccessBenchmarkRowExecutor:
		return ScalarFuncExecutor{
			RowExecutor: func(values []driver.Value) (any, error) {
				return udf.transform(values[0].(T)), nil
			},
		}
	case vectorAccessBenchmarkChunkRows:
		return ScalarFuncExecutor{
			ChunkContextExecutor: func(_ context.Context, state *ChunkIteratorState) error {
				for row, err := range state.Rows() {
					if err != nil {
						return err
					}
					value := (*row.GetValuePtr(0)).(T)
					if err = row.SetResult(udf.transform(value)); err != nil {
						return err
					}
				}
				return nil
			},
		}
	case vectorAccessBenchmarkVector:
		return ScalarFuncExecutor{
			ChunkContextExecutor: func(_ context.Context, state *ChunkIteratorState) error {
				inputVector, err := state.GetInputChunk().GetVector(0)
				if err != nil {
					return err
				}
				input, err := GetVectorView[T](inputVector)
				if err != nil {
					return err
				}
				output, err := GetVectorWriter[T](state.GetResultVector())
				if err != nil {
					return err
				}

				for rowIdx := range input.Len() {
					value, valid, err := input.Get(rowIdx)
					if err != nil {
						return err
					}
					if !valid {
						continue
					}
					if err = output.Set(rowIdx, udf.transform(value)); err != nil {
						return err
					}
				}
				return nil
			},
		}
	default:
		return ScalarFuncExecutor{}
	}
}

type vectorAccessBenchmarkCase struct {
	name     string
	function string
	mode     vectorAccessBenchmarkMode
}

func vectorAccessBenchmarkCases(prefix string, includeChunkRows bool) []vectorAccessBenchmarkCase {
	cases := []vectorAccessBenchmarkCase{
		{
			name:     "RowExecutor",
			function: prefix + "_row_executor_transform",
			mode:     vectorAccessBenchmarkRowExecutor,
		},
	}
	if includeChunkRows {
		cases = append(cases, vectorAccessBenchmarkCase{
			name:     "ChunkRows",
			function: prefix + "_chunk_rows_transform",
			mode:     vectorAccessBenchmarkChunkRows,
		})
	}
	return append(cases, vectorAccessBenchmarkCase{
		name:     "VectorAccess",
		function: prefix + "_vector_access_transform",
		mode:     vectorAccessBenchmarkVector,
	})
}

const vectorAccessBenchmarkRowCount = 2_000_000

var (
	varcharTransformBenchmarkSink    int64
	fixedWidthTransformBenchmarkSink float64
)

func transformBenchmarkVarchar(value string) string {
	value = strings.TrimSpace(value)
	return strings.Map(func(r rune) rune {
		if r == '-' {
			return '_'
		}
		return unicode.ToUpper(r)
	}, value)
}

func BenchmarkVarcharTransformUDF(b *testing.B) {
	db := openDbWrapper(b, ``)
	defer closeDbWrapper(b, db)
	conn := openConnWrapper(b, db, context.Background())
	defer closeConnWrapper(b, conn)
	_, err := conn.ExecContext(context.Background(), `SET threads = 1`)
	require.NoError(b, err)

	info := mustTypeInfo(b, TYPE_VARCHAR)
	benchmarks := vectorAccessBenchmarkCases("varchar", false)
	for _, benchmark := range benchmarks {
		require.NoError(b, RegisterScalarUDF(
			conn,
			benchmark.function,
			&vectorAccessBenchmarkUDF[string]{
				info:      info,
				mode:      benchmark.mode,
				transform: transformBenchmarkVarchar,
			},
		))
	}

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf(`
		CREATE TABLE varchar_transform_benchmark AS
		SELECT CASE i %% 6
			WHEN 0 THEN NULL::VARCHAR
			WHEN 1 THEN ''
			WHEN 2 THEN '  customer-' || i::VARCHAR || '-alpha  '
			WHEN 3 THEN repeat('duckdb-go-', 4) || i::VARCHAR
			WHEN 4 THEN '  München-' || i::VARCHAR || '-straße  '
			ELSE repeat('long-varchar-value-', 8) || i::VARCHAR
		END AS value
		FROM range(%d) values(i)
	`, vectorAccessBenchmarkRowCount))
	require.NoError(b, err)

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			query := `SELECT coalesce(sum(length(` + benchmark.function +
				`(value))), 0) FROM varchar_transform_benchmark`
			stmt, prepareErr := conn.PrepareContext(context.Background(), query)
			require.NoError(b, prepareErr)
			defer func() {
				require.NoError(b, stmt.Close())
			}()

			require.NoError(b, stmt.QueryRowContext(context.Background()).Scan(&varcharTransformBenchmarkSink))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := stmt.QueryRowContext(context.Background()).Scan(&varcharTransformBenchmarkSink); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(vectorAccessBenchmarkRowCount, "rows/op")
		})
	}
}

func BenchmarkFixedWidthTransformUDF(b *testing.B) {
	b.Run("INTEGER", func(b *testing.B) {
		benchmarkFixedWidthTransformUDF(
			b,
			TYPE_INTEGER,
			`((i % 1000001) - 500000)::INTEGER`,
			`value * 31 + 17`,
			func(value int32) int32 {
				return value*31 + 17
			},
		)
	})

	b.Run("DOUBLE", func(b *testing.B) {
		benchmarkFixedWidthTransformUDF(
			b,
			TYPE_DOUBLE,
			`((i % 1000001) - 500000)::DOUBLE / 8.0`,
			`value * 1.25 + 3.5`,
			func(value float64) float64 {
				return value*1.25 + 3.5
			},
		)
	})
}

func benchmarkFixedWidthTransformUDF[T vectorValue](
	b *testing.B,
	typ Type,
	inputExpression string,
	expectedExpression string,
	transform func(T) T,
) {
	b.Helper()

	db := openDbWrapper(b, ``)
	defer closeDbWrapper(b, db)
	conn := openConnWrapper(b, db, context.Background())
	defer closeConnWrapper(b, conn)
	_, err := conn.ExecContext(context.Background(), `SET threads = 1`)
	require.NoError(b, err)

	info := mustTypeInfo(b, typ)
	benchmarks := vectorAccessBenchmarkCases("fixed_width", true)
	// The fixed-width benchmark uses only valid inputs. Special NULL handling
	// avoids the post-callback NULL scan that applies only to chunk executors.
	for _, benchmark := range benchmarks {
		require.NoError(b, RegisterScalarUDF(
			conn,
			benchmark.function,
			&vectorAccessBenchmarkUDF[T]{
				info:                info,
				mode:                benchmark.mode,
				transform:           transform,
				specialNullHandling: true,
			},
		))
	}

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf(`
		CREATE TABLE fixed_width_transform_benchmark AS
		SELECT %s AS value
		FROM range(%d) values(i)
	`, inputExpression, vectorAccessBenchmarkRowCount))
	require.NoError(b, err)

	var expectedChecksum float64
	err = conn.QueryRowContext(context.Background(), fmt.Sprintf(`
		SELECT CAST(coalesce(sum(%s), 0) AS DOUBLE)
		FROM fixed_width_transform_benchmark
	`, expectedExpression)).Scan(&expectedChecksum)
	require.NoError(b, err)

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			query := fmt.Sprintf(
				`SELECT CAST(coalesce(sum(%s(value)), 0) AS DOUBLE) FROM fixed_width_transform_benchmark`,
				benchmark.function,
			)
			stmt, prepareErr := conn.PrepareContext(context.Background(), query)
			require.NoError(b, prepareErr)
			defer func() {
				require.NoError(b, stmt.Close())
			}()

			var warmChecksum float64
			require.NoError(b, stmt.QueryRowContext(context.Background()).Scan(&warmChecksum))
			require.Equal(b, expectedChecksum, warmChecksum)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := stmt.QueryRowContext(context.Background()).Scan(&fixedWidthTransformBenchmarkSink); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(vectorAccessBenchmarkRowCount, "rows/op")
		})
	}
}
