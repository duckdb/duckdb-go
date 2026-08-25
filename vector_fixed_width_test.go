package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type (
	namedFixedWidthBool    bool
	namedFixedWidthInt32   int32
	namedFixedWidthUint64  uint64
	namedFixedWidthFloat64 float64
)

func TestFixedWidthVectorAccess(t *testing.T) {
	t.Run("BOOLEAN", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_BOOLEAN, []bool{false, true, false})
	})
	t.Run("TINYINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_TINYINT, []int8{-128, 0, 127})
	})
	t.Run("SMALLINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_SMALLINT, []int16{-32_768, 0, 32_767})
	})
	t.Run("INTEGER", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_INTEGER, []int32{-2_147_483_648, 0, 2_147_483_647})
	})
	t.Run("BIGINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_BIGINT, []int64{-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807})
	})
	t.Run("UTINYINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_UTINYINT, []uint8{0, 127, 255})
	})
	t.Run("USMALLINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_USMALLINT, []uint16{0, 32_768, 65_535})
	})
	t.Run("UINTEGER", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_UINTEGER, []uint32{0, 2_147_483_648, 4_294_967_295})
	})
	t.Run("UBIGINT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_UBIGINT, []uint64{0, 9_223_372_036_854_775_808, 18_446_744_073_709_551_615})
	})
	t.Run("FLOAT", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_FLOAT, []float32{-123.5, 0, 987.25})
	})
	t.Run("DOUBLE", func(t *testing.T) {
		testFixedWidthVectorAccess(t, TYPE_DOUBLE, []float64{-1.25e100, 0, 1.5e100})
	})
}

func testFixedWidthVectorAccess[T vectorValue](t *testing.T, typ Type, values []T) {
	t.Helper()

	t.Run("View", func(t *testing.T) {
		testFixedWidthVectorView(t, typ, values)
	})
	t.Run("Writer", func(t *testing.T) {
		testFixedWidthVectorWriter(t, typ, values)
	})
}

func testFixedWidthVectorView[T vectorValue](t *testing.T, typ Type, values []T) {
	t.Helper()

	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, typ))
	for rowIdx, value := range values {
		require.NoError(t, chunk.SetValue(0, rowIdx, value))
	}
	nullRow := len(values)
	require.NoError(t, chunk.SetValue(0, nullRow, nil))
	require.NoError(t, chunk.SetSize(nullRow+1))

	view, err := GetVectorView[T](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	require.Equal(t, nullRow+1, view.Len())

	for rowIdx, expected := range values {
		actual, valid, getErr := view.GetValueBorrowed(rowIdx)
		require.NoError(t, getErr)
		require.True(t, valid)
		require.Equal(t, expected, actual)
	}

	actual, valid, err := view.GetValueBorrowed(nullRow)
	require.NoError(t, err)
	require.False(t, valid)
	var zero T
	require.Equal(t, zero, actual)
}

func testFixedWidthVectorWriter[T vectorValue](t *testing.T, typ Type, values []T) {
	t.Helper()

	state, _, output := newVectorWriterTestState(t, mustTypeInfo(t, typ), len(values)+1, false)
	writer, err := GetVectorWriter[T](state.GetResultVector())
	require.NoError(t, err)
	require.Equal(t, len(values)+1, writer.Len())

	for rowIdx, value := range values {
		require.NoError(t, writer.Set(rowIdx, value))
	}
	nullRow := len(values)
	require.NoError(t, writer.SetNull(nullRow))

	for rowIdx, expected := range values {
		actual, getErr := output.GetValue(0, rowIdx)
		require.NoError(t, getErr)
		require.Equal(t, any(expected), actual)
	}
	actual, err := output.GetValue(0, nullRow)
	require.NoError(t, err)
	require.Nil(t, actual)

	require.NoError(t, writer.SetNull(0))
	actual, err = output.GetValue(0, 0)
	require.NoError(t, err)
	require.Nil(t, actual)
	require.NoError(t, writer.Set(0, values[0]))
	actual, err = output.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, any(values[0]), actual)
}

func TestFixedWidthVectorAccessNamedTypes(t *testing.T) {
	t.Run("BOOLEAN", func(t *testing.T) {
		testNamedFixedWidthVectorAccess(t, TYPE_BOOLEAN, true, namedFixedWidthBool(true), namedFixedWidthBool(false), false)
	})
	t.Run("INTEGER", func(t *testing.T) {
		testNamedFixedWidthVectorAccess(t, TYPE_INTEGER, int32(-1234), namedFixedWidthInt32(-1234), namedFixedWidthInt32(4321), int32(4321))
	})
	t.Run("UBIGINT", func(t *testing.T) {
		testNamedFixedWidthVectorAccess(t, TYPE_UBIGINT, uint64(1234), namedFixedWidthUint64(1234), namedFixedWidthUint64(4321), uint64(4321))
	})
	t.Run("DOUBLE", func(t *testing.T) {
		testNamedFixedWidthVectorAccess(t, TYPE_DOUBLE, 12.5, namedFixedWidthFloat64(12.5), namedFixedWidthFloat64(-43.25), -43.25)
	})
}

func testNamedFixedWidthVectorAccess[T vectorValue](
	t *testing.T,
	typ Type,
	input any,
	expected T,
	output T,
	expectedOutput any,
) {
	t.Helper()

	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, typ))
	require.NoError(t, chunk.SetValue(0, 0, input))
	require.NoError(t, chunk.SetSize(1))

	view, err := GetVectorView[T](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	value, valid, err := view.GetValueBorrowed(0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, expected, value)

	state, _, outputChunk := newVectorWriterTestState(t, mustTypeInfo(t, typ), 1, false)
	writer, err := GetVectorWriter[T](state.GetResultVector())
	require.NoError(t, err)
	require.NoError(t, writer.Set(0, output))
	written, err := outputChunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, expectedOutput, written)
}

func TestFixedWidthVectorAccessTypeMismatch(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTEGER))
	require.NoError(t, chunk.SetSize(1))

	_, err := GetVectorView[uint32](mustGetVector(t, chunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be read as Go uint32")

	state, _, _ := newVectorWriterTestState(t, mustTypeInfo(t, TYPE_INTEGER), 1, false)
	_, err = GetVectorWriter[uint32](state.GetResultVector())
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be written as Go uint32")
}

func TestFixedWidthVectorAccessRejectsDecimalStorage(t *testing.T) {
	decimalInfo, err := NewDecimalInfo(9, 2)
	require.NoError(t, err)
	chunk := newVectorViewTestChunk(t, decimalInfo)
	require.NoError(t, chunk.SetSize(1))

	_, err = GetVectorView[int32](mustGetVector(t, chunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB DECIMAL cannot be read as Go int32")

	state, _, _ := newVectorWriterTestState(t, decimalInfo, 1, false)
	_, err = GetVectorWriter[int32](state.GetResultVector())
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB DECIMAL cannot be written as Go int32")
}

type fixedWidthVectorIdentityUDF[T vectorValue] struct {
	info TypeInfo
}

func (udf *fixedWidthVectorIdentityUDF[T]) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{udf.info},
		ResultTypeInfo: udf.info,
	}
}

func (*fixedWidthVectorIdentityUDF[T]) Executor() ScalarFuncExecutor {
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
				if err = output.Set(rowIdx, value); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func TestFixedWidthVectorScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	t.Run("BOOLEAN", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[bool](t, conn, "fixed_width_boolean", TYPE_BOOLEAN, `(false::BOOLEAN), (true::BOOLEAN), (NULL::BOOLEAN)`, 3)
	})
	t.Run("TINYINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[int8](t, conn, "fixed_width_tinyint", TYPE_TINYINT, `(-12::TINYINT), (0::TINYINT), (34::TINYINT), (NULL::TINYINT)`, 4)
	})
	t.Run("SMALLINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[int16](t, conn, "fixed_width_smallint", TYPE_SMALLINT, `(-1200::SMALLINT), (0::SMALLINT), (3400::SMALLINT), (NULL::SMALLINT)`, 4)
	})
	t.Run("INTEGER", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[int32](t, conn, "fixed_width_integer", TYPE_INTEGER, `(-120000::INTEGER), (0::INTEGER), (340000::INTEGER), (NULL::INTEGER)`, 4)
	})
	t.Run("BIGINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[int64](t, conn, "fixed_width_bigint", TYPE_BIGINT, `(-12000000000::BIGINT), (0::BIGINT), (34000000000::BIGINT), (NULL::BIGINT)`, 4)
	})
	t.Run("UTINYINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[uint8](t, conn, "fixed_width_utinyint", TYPE_UTINYINT, `(0::UTINYINT), (12::UTINYINT), (34::UTINYINT), (NULL::UTINYINT)`, 4)
	})
	t.Run("USMALLINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[uint16](t, conn, "fixed_width_usmallint", TYPE_USMALLINT, `(0::USMALLINT), (1200::USMALLINT), (3400::USMALLINT), (NULL::USMALLINT)`, 4)
	})
	t.Run("UINTEGER", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[uint32](t, conn, "fixed_width_uinteger", TYPE_UINTEGER, `(0::UINTEGER), (120000::UINTEGER), (340000::UINTEGER), (NULL::UINTEGER)`, 4)
	})
	t.Run("UBIGINT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[uint64](t, conn, "fixed_width_ubigint", TYPE_UBIGINT, `(0::UBIGINT), (12000000000::UBIGINT), (34000000000::UBIGINT), (NULL::UBIGINT)`, 4)
	})
	t.Run("FLOAT", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[float32](t, conn, "fixed_width_float", TYPE_FLOAT, `(-12.5::FLOAT), (0::FLOAT), (34.25::FLOAT), (NULL::FLOAT)`, 4)
	})
	t.Run("DOUBLE", func(t *testing.T) {
		testFixedWidthVectorScalarUDF[float64](t, conn, "fixed_width_double", TYPE_DOUBLE, `(-12.5::DOUBLE), (0::DOUBLE), (34.25::DOUBLE), (NULL::DOUBLE)`, 4)
	})
}

func TestFixedWidthVectorScalarUDFMultipleChunks(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	const name = "fixed_width_integer_multiple_chunks"
	udf := &fixedWidthVectorIdentityUDF[int32]{info: mustTypeInfo(t, TYPE_INTEGER)}
	require.NoError(t, RegisterScalarUDF(conn, name, udf))

	rowCount := GetDataChunkCapacity()*2 + 17
	query := fmt.Sprintf(`
		WITH source AS (
			SELECT CASE
				WHEN i %% 257 = 0 THEN NULL::INTEGER
				ELSE ((i %% 100000) - 50000)::INTEGER
			END AS value
			FROM range(?) values(i)
		),
		results AS (
			SELECT value, %s(value) AS actual
			FROM source
		)
		SELECT count(*), count(*) FILTER (WHERE actual IS DISTINCT FROM value)
		FROM results
	`, name)

	var actualCount, mismatchCount int
	require.NoError(t, conn.QueryRowContext(context.Background(), query, rowCount).Scan(&actualCount, &mismatchCount))
	require.Equal(t, rowCount, actualCount)
	require.Zero(t, mismatchCount)
}

func testFixedWidthVectorScalarUDF[T vectorValue](
	t *testing.T,
	conn *sql.Conn,
	name string,
	typ Type,
	valuesSQL string,
	expectedCount int,
) {
	t.Helper()

	udf := &fixedWidthVectorIdentityUDF[T]{info: mustTypeInfo(t, typ)}
	require.NoError(t, RegisterScalarUDF(conn, name, udf))

	query := fmt.Sprintf(`
		WITH source(value) AS (VALUES %s),
		results AS (
			SELECT value, %s(value) AS actual
			FROM source
		)
		SELECT count(*), count(*) FILTER (WHERE actual IS DISTINCT FROM value)
		FROM results
	`, valuesSQL, name)

	var rowCount, mismatchCount int
	require.NoError(t, conn.QueryRowContext(context.Background(), query).Scan(&rowCount, &mismatchCount))
	require.Equal(t, expectedCount, rowCount)
	require.Zero(t, mismatchCount)
}
