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

type varcharTransformBenchmarkUDF struct {
	info      TypeInfo
	useVector bool
}

func (udf *varcharTransformBenchmarkUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{udf.info},
		ResultTypeInfo: udf.info,
	}
}

func (udf *varcharTransformBenchmarkUDF) Executor() ScalarFuncExecutor {
	if !udf.useVector {
		return ScalarFuncExecutor{
			RowExecutor: func(values []driver.Value) (any, error) {
				return transformBenchmarkVarchar(values[0].(string)), nil
			},
		}
	}

	return ScalarFuncExecutor{
		ChunkContextExecutor: func(_ context.Context, state *ChunkIteratorState) error {
			inputVector, err := state.GetInputChunk().GetVector(0)
			if err != nil {
				return err
			}
			input, err := GetVectorView[string](inputVector)
			if err != nil {
				return err
			}
			output, err := GetVectorWriter[string](state.GetResultVector())
			if err != nil {
				return err
			}

			for row := range input.Len() {
				value, valid, err := input.GetValueBorrowed(row)
				if err != nil {
					return err
				}
				if !valid {
					// Default NULL handling sets this result to NULL after the callback.
					continue
				}
				if err = output.Set(row, transformBenchmarkVarchar(value)); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func transformBenchmarkVarchar(value string) string {
	value = strings.TrimSpace(value)
	return strings.Map(func(r rune) rune {
		if r == '-' {
			return '_'
		}
		return unicode.ToUpper(r)
	}, value)
}

var varcharTransformBenchmarkSink int64

const varcharTransformBenchmarkRowCount = 2_000_000

func BenchmarkVarcharTransformUDF(b *testing.B) {
	db := openDbWrapper(b, ``)
	defer closeDbWrapper(b, db)
	conn := openConnWrapper(b, db, context.Background())
	defer closeConnWrapper(b, conn)
	_, err := conn.ExecContext(context.Background(), `SET threads = 1`)
	require.NoError(b, err)

	info := mustTypeInfo(b, TYPE_VARCHAR)
	require.NoError(b, RegisterScalarUDF(conn, "varchar_rows_transform", &varcharTransformBenchmarkUDF{
		info: info,
	}))
	require.NoError(b, RegisterScalarUDF(conn, "varchar_vector_transform", &varcharTransformBenchmarkUDF{
		info:      info,
		useVector: true,
	}))

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
	`, varcharTransformBenchmarkRowCount))
	require.NoError(b, err)

	benchmarks := []struct {
		name     string
		function string
	}{
		{name: "RowExecutor", function: "varchar_rows_transform"},
		{name: "ChunkVector", function: "varchar_vector_transform"},
	}

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
			b.ReportMetric(varcharTransformBenchmarkRowCount, "rows/op")
		})
	}
}
