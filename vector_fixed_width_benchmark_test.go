package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type fixedWidthBenchmarkMode uint8

const (
	fixedWidthBenchmarkRowExecutor fixedWidthBenchmarkMode = iota
	fixedWidthBenchmarkChunkRows
	fixedWidthBenchmarkChunkVector
)

type fixedWidthTransformBenchmarkUDF[T vectorValue] struct {
	info      TypeInfo
	mode      fixedWidthBenchmarkMode
	transform func(T) T
}

func (udf *fixedWidthTransformBenchmarkUDF[T]) Config() ScalarFuncConfig {
	// The benchmark uses only valid inputs. Special NULL handling avoids the
	// post-callback NULL scan that applies only to chunk executors.
	return ScalarFuncConfig{
		InputTypeInfos:      []TypeInfo{udf.info},
		ResultTypeInfo:      udf.info,
		SpecialNullHandling: true,
	}
}

func (udf *fixedWidthTransformBenchmarkUDF[T]) Executor() ScalarFuncExecutor {
	switch udf.mode {
	case fixedWidthBenchmarkRowExecutor:
		return ScalarFuncExecutor{
			RowExecutor: func(values []driver.Value) (any, error) {
				return udf.transform(values[0].(T)), nil
			},
		}
	case fixedWidthBenchmarkChunkRows:
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
	case fixedWidthBenchmarkChunkVector:
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
					value, valid, err := input.GetValueBorrowed(rowIdx)
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

const fixedWidthTransformBenchmarkRowCount = 2_000_000

var fixedWidthTransformBenchmarkSink float64

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
	benchmarks := []struct {
		name string
		mode fixedWidthBenchmarkMode
	}{
		{name: "RowExecutor", mode: fixedWidthBenchmarkRowExecutor},
		{name: "ChunkRows", mode: fixedWidthBenchmarkChunkRows},
		{name: "ChunkVector", mode: fixedWidthBenchmarkChunkVector},
	}
	for _, benchmark := range benchmarks {
		require.NoError(b, RegisterScalarUDF(
			conn,
			fixedWidthBenchmarkFunctionName(benchmark.mode),
			&fixedWidthTransformBenchmarkUDF[T]{
				info:      info,
				mode:      benchmark.mode,
				transform: transform,
			},
		))
	}

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf(`
		CREATE TABLE fixed_width_transform_benchmark AS
		SELECT %s AS value
		FROM range(%d) values(i)
	`, inputExpression, fixedWidthTransformBenchmarkRowCount))
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
				fixedWidthBenchmarkFunctionName(benchmark.mode),
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
			b.ReportMetric(fixedWidthTransformBenchmarkRowCount, "rows/op")
		})
	}
}

func fixedWidthBenchmarkFunctionName(mode fixedWidthBenchmarkMode) string {
	switch mode {
	case fixedWidthBenchmarkRowExecutor:
		return "fixed_width_row_executor_transform"
	case fixedWidthBenchmarkChunkRows:
		return "fixed_width_chunk_rows_transform"
	case fixedWidthBenchmarkChunkVector:
		return "fixed_width_chunk_vector_transform"
	default:
		return ""
	}
}
