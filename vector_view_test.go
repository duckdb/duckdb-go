package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

type (
	namedVarchar           string
	namedFixedWidthBool    bool
	namedFixedWidthInt32   int32
	namedFixedWidthUint64  uint64
	namedFixedWidthFloat64 float64
)

func newVectorViewTestChunk(t testing.TB, infos ...TypeInfo) *DataChunk {
	t.Helper()

	logicalTypes := make([]mapping.LogicalType, len(infos))
	for i := range infos {
		logicalTypes[i] = infos[i].logicalType()
	}

	chunk := &DataChunk{}
	err := chunk.initFromTypes(logicalTypes, true)
	for i := range logicalTypes {
		mapping.DestroyLogicalType(&logicalTypes[i])
	}
	require.NoError(t, err)
	t.Cleanup(chunk.close)
	return chunk
}

func mustGetVector(t testing.TB, chunk *DataChunk, column int) Vector {
	t.Helper()

	vector, err := chunk.GetVector(column)
	require.NoError(t, err)
	return vector
}

func TestDuckDBStringTViewLayout(t *testing.T) {
	require.Equal(t, uintptr(16), unsafe.Sizeof(mapping.StringT{}))
}

func TestVarcharVectorView(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_VARCHAR))
	values := []string{
		"",
		"twelve-bytes",
		"this string is longer than DuckDB's inline string storage",
		"embedded\x00nul",
	}
	for i := range values {
		require.NoError(t, SetChunkValue(*chunk, 0, i, values[i]))
	}
	nullRow := len(values)
	require.NoError(t, chunk.SetValue(0, nullRow, nil))
	require.NoError(t, chunk.SetSize(nullRow+1))

	view, err := GetVectorView[string](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	require.Equal(t, len(values)+1, view.Len())

	for i := range values {
		value, valid, getErr := view.GetValue(i)
		require.NoError(t, getErr)
		require.True(t, valid)
		require.Equal(t, values[i], value)

		owned, getErr := chunk.GetValue(0, i)
		require.NoError(t, getErr)
		require.Equal(t, values[i], owned)
		if values[i] != "" {
			require.NotEqual(t, stringDataAddress(value), stringDataAddress(owned.(string)))
		}
	}

	value, valid, err := view.GetValue(nullRow)
	require.NoError(t, err)
	require.False(t, valid)
	require.Empty(t, value)

	namedView, err := GetVectorView[namedVarchar](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	namedValue, valid, err := namedView.GetValue(1)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, namedVarchar(values[1]), namedValue)
}

func TestVectorViewValidation(t *testing.T) {
	intInfo := mustTypeInfo(t, TYPE_INTEGER)
	stringInfo := mustTypeInfo(t, TYPE_VARCHAR)
	chunk := newVectorViewTestChunk(t, intInfo, stringInfo)
	require.NoError(t, chunk.SetSize(1))

	_, err := GetVectorView[string](mustGetVector(t, chunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be read as Go string")

	_, err = GetVectorView[uint32](mustGetVector(t, chunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be read as Go uint32")

	_, err = chunk.GetVector(-1)
	require.ErrorIs(t, err, errAPI)

	_, err = chunk.GetVector(2)
	require.ErrorIs(t, err, errAPI)

	var nilChunk *DataChunk
	_, err = nilChunk.GetVector(0)
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errNilDataChunk)

	_, err = GetVectorView[string](Vector{})
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errUninitializedVectorView)

	var zeroView VectorView[string]
	_, _, err = zeroView.GetValue(0)
	require.ErrorIs(t, err, errUninitializedVectorView)

	var nilState *ChunkIteratorState
	_, err = nilState.GetInputChunk().GetVector(0)
	require.ErrorIs(t, err, errNilDataChunk)

	view, err := GetVectorView[string](mustGetVector(t, chunk, 1))
	require.NoError(t, err)
	_, _, err = view.GetValue(-1)
	require.ErrorContains(t, err, rowIndexErrMsg)
	_, _, err = view.GetValue(view.Len())
	require.ErrorContains(t, err, rowIndexErrMsg)

	chunk.projection = []int{1}
	projected, err := GetVectorView[string](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	require.Equal(t, 1, projected.Len())

	decimalInfo, err := NewDecimalInfo(9, 2)
	require.NoError(t, err)
	decimalChunk := newVectorViewTestChunk(t, decimalInfo)
	require.NoError(t, decimalChunk.SetSize(1))
	_, err = GetVectorView[int32](mustGetVector(t, decimalChunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB DECIMAL cannot be read as Go int32")
}

func TestScalarUDFInputChunk(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_VARCHAR))
	require.NoError(t, chunk.SetValue(0, 0, "input"))
	require.NoError(t, chunk.SetSize(1))

	state := &ChunkIteratorState{input: chunk}
	inputChunk := state.GetInputChunk()
	require.Same(t, chunk, inputChunk)
	require.Equal(t, 1, inputChunk.GetSize())
	require.Equal(t, 1, inputChunk.ColumnCount())

	value, err := inputChunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, "input", value)

	view, err := GetVectorView[string](mustGetVector(t, inputChunk, 0))
	require.NoError(t, err)
	viewValue, valid, err := view.GetValue(0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, "input", viewValue)
}

func TestVarcharVectorViewRejectsJSONAlias(t *testing.T) {
	logicalType := mapping.CreateLogicalType(TYPE_VARCHAR)
	mapping.LogicalTypeSetAlias(logicalType, aliasJSON)
	defer mapping.DestroyLogicalType(&logicalType)

	chunk := &DataChunk{}
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	t.Cleanup(chunk.close)
	require.NoError(t, SetChunkValue(*chunk, 0, 0, `{"answer":42}`))
	require.NoError(t, chunk.SetSize(1))

	_, err := GetVectorView[string](mustGetVector(t, chunk, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB JSON cannot be read as Go string")

	owned, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"answer": float64(42)}, owned)
}

type vectorViewIdentityUDF[T vectorValue] struct {
	info                TypeInfo
	specialNullHandling bool
	nullValue           T
}

func (udf *vectorViewIdentityUDF[T]) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos:      []TypeInfo{udf.info},
		ResultTypeInfo:      udf.info,
		SpecialNullHandling: udf.specialNullHandling,
	}
}

func (udf *vectorViewIdentityUDF[T]) Executor() ScalarFuncExecutor {
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
			for row := range input.Len() {
				value, valid, err := input.GetValue(row)
				if err != nil {
					return err
				}
				if !valid {
					if !udf.specialNullHandling {
						continue
					}
					value = udf.nullValue
				}
				if err = output.Set(row, value); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func TestVectorViewScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	t.Run("VARCHAR", func(t *testing.T) {
		testVectorViewScalarUDF[string](t, conn, "vector_view_varchar", TYPE_VARCHAR, `('short'::VARCHAR), ('a value longer than twelve bytes'::VARCHAR), (NULL::VARCHAR)`, 3)
	})
	t.Run("BOOLEAN", func(t *testing.T) {
		testVectorViewScalarUDF[bool](t, conn, "vector_view_boolean", TYPE_BOOLEAN, `(false::BOOLEAN), (true::BOOLEAN), (NULL::BOOLEAN)`, 3)
	})
	t.Run("TINYINT", func(t *testing.T) {
		testVectorViewScalarUDF[int8](t, conn, "vector_view_tinyint", TYPE_TINYINT, `(-12::TINYINT), (0::TINYINT), (34::TINYINT), (NULL::TINYINT)`, 4)
	})
	t.Run("SMALLINT", func(t *testing.T) {
		testVectorViewScalarUDF[int16](t, conn, "vector_view_smallint", TYPE_SMALLINT, `(-1200::SMALLINT), (0::SMALLINT), (3400::SMALLINT), (NULL::SMALLINT)`, 4)
	})
	t.Run("INTEGER", func(t *testing.T) {
		testVectorViewScalarUDF[int32](t, conn, "vector_view_integer", TYPE_INTEGER, `(-120000::INTEGER), (0::INTEGER), (340000::INTEGER), (NULL::INTEGER)`, 4)
	})
	t.Run("BIGINT", func(t *testing.T) {
		testVectorViewScalarUDF[int64](t, conn, "vector_view_bigint", TYPE_BIGINT, `(-12000000000::BIGINT), (0::BIGINT), (34000000000::BIGINT), (NULL::BIGINT)`, 4)
	})
	t.Run("UTINYINT", func(t *testing.T) {
		testVectorViewScalarUDF[uint8](t, conn, "vector_view_utinyint", TYPE_UTINYINT, `(0::UTINYINT), (12::UTINYINT), (34::UTINYINT), (NULL::UTINYINT)`, 4)
	})
	t.Run("USMALLINT", func(t *testing.T) {
		testVectorViewScalarUDF[uint16](t, conn, "vector_view_usmallint", TYPE_USMALLINT, `(0::USMALLINT), (1200::USMALLINT), (3400::USMALLINT), (NULL::USMALLINT)`, 4)
	})
	t.Run("UINTEGER", func(t *testing.T) {
		testVectorViewScalarUDF[uint32](t, conn, "vector_view_uinteger", TYPE_UINTEGER, `(0::UINTEGER), (120000::UINTEGER), (340000::UINTEGER), (NULL::UINTEGER)`, 4)
	})
	t.Run("UBIGINT", func(t *testing.T) {
		testVectorViewScalarUDF[uint64](t, conn, "vector_view_ubigint", TYPE_UBIGINT, `(0::UBIGINT), (12000000000::UBIGINT), (34000000000::UBIGINT), (NULL::UBIGINT)`, 4)
	})
	t.Run("FLOAT", func(t *testing.T) {
		testVectorViewScalarUDF[float32](t, conn, "vector_view_float", TYPE_FLOAT, `(-12.5::FLOAT), (0::FLOAT), (34.25::FLOAT), (NULL::FLOAT)`, 4)
	})
	t.Run("DOUBLE", func(t *testing.T) {
		testVectorViewScalarUDF[float64](t, conn, "vector_view_double", TYPE_DOUBLE, `(-12.5::DOUBLE), (0::DOUBLE), (34.25::DOUBLE), (NULL::DOUBLE)`, 4)
	})

	t.Run("NAMED_BOOLEAN", func(t *testing.T) {
		testVectorViewScalarUDF[namedFixedWidthBool](t, conn, "vector_view_named_boolean", TYPE_BOOLEAN, `(false::BOOLEAN), (true::BOOLEAN), (NULL::BOOLEAN)`, 3)
	})
	t.Run("NAMED_INTEGER", func(t *testing.T) {
		testVectorViewScalarUDF[namedFixedWidthInt32](t, conn, "vector_view_named_integer", TYPE_INTEGER, `(-1234::INTEGER), (4321::INTEGER), (NULL::INTEGER)`, 3)
	})
	t.Run("NAMED_UBIGINT", func(t *testing.T) {
		testVectorViewScalarUDF[namedFixedWidthUint64](t, conn, "vector_view_named_ubigint", TYPE_UBIGINT, `(1234::UBIGINT), (4321::UBIGINT), (NULL::UBIGINT)`, 3)
	})
	t.Run("NAMED_DOUBLE", func(t *testing.T) {
		testVectorViewScalarUDF[namedFixedWidthFloat64](t, conn, "vector_view_named_double", TYPE_DOUBLE, `(12.5::DOUBLE), (-43.25::DOUBLE), (NULL::DOUBLE)`, 3)
	})
}

func testVectorViewScalarUDF[T vectorValue](
	t *testing.T,
	conn *sql.Conn,
	name string,
	typ Type,
	valuesSQL string,
	expectedCount int,
) {
	t.Helper()

	udf := &vectorViewIdentityUDF[T]{info: mustTypeInfo(t, typ)}
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

func TestVectorViewScalarUDFMultipleChunks(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	udf := &vectorViewIdentityUDF[string]{info: mustTypeInfo(t, TYPE_VARCHAR)}
	require.NoError(t, RegisterScalarUDF(conn, "vector_view_identity_multiple_chunks", udf))

	rowCount := GetDataChunkCapacity()*2 + 17
	var actualCount, mismatchCount int
	err := db.QueryRow(`
		WITH source AS (
			SELECT CASE
				WHEN i % 257 = 0 THEN NULL::VARCHAR
				WHEN i % 4 = 0 THEN ''
				WHEN i % 4 = 1 THEN 'short-' || i::VARCHAR
				ELSE repeat('long-value-', 3) || i::VARCHAR
			END AS value
			FROM range(?) values(i)
		), results AS (
			SELECT value, vector_view_identity_multiple_chunks(value) AS actual
			FROM source
		)
		SELECT count(*), count(*) FILTER (WHERE actual IS DISTINCT FROM value)
		FROM results
	`, rowCount).Scan(&actualCount, &mismatchCount)
	require.NoError(t, err)
	require.Equal(t, rowCount, actualCount)
	require.Zero(t, mismatchCount)
}

func TestVectorViewScalarUDFSpecialNullHandling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	udf := &vectorViewIdentityUDF[string]{
		info:                mustTypeInfo(t, TYPE_VARCHAR),
		specialNullHandling: true,
		nullValue:           "handled NULL",
	}
	require.NoError(t, RegisterScalarUDF(conn, "vector_view_identity_special", udf))

	var result string
	require.NoError(
		t,
		db.QueryRow(`SELECT vector_view_identity_special(NULL)`).Scan(&result),
	)
	require.Equal(t, "handled NULL", result)
}

type mixedVectorAccessUDF struct {
	varcharInfo TypeInfo
	integerInfo TypeInfo
	structInfo  TypeInfo
}

func (udf *mixedVectorAccessUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{
			udf.varcharInfo,
			udf.integerInfo,
			udf.structInfo,
		},
		ResultTypeInfo: udf.varcharInfo,
	}
}

func (*mixedVectorAccessUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		ChunkContextExecutor: func(_ context.Context, state *ChunkIteratorState) error {
			inputChunk := state.GetInputChunk()
			inputVector, err := inputChunk.GetVector(0)
			if err != nil {
				return err
			}
			varcharView, err := GetVectorView[string](inputVector)
			if err != nil {
				return err
			}

			output, err := GetVectorWriter[string](state.GetResultVector())
			if err != nil {
				return err
			}

			for row := range varcharView.Len() {
				varcharValue, valid, err := varcharView.GetValue(row)
				if err != nil {
					return err
				}
				if !valid {
					// Default NULL handling sets the result to NULL.
					continue
				}

				integerValue, err := inputChunk.GetValue(1, row)
				if err != nil {
					return err
				}
				structValue, err := inputChunk.GetValue(2, row)
				if err != nil {
					return err
				}
				if integerValue == nil || structValue == nil {
					// Default NULL handling sets the result to NULL.
					continue
				}

				fields := structValue.(map[string]any)
				result := fmt.Sprintf(
					"%s:%d:%s",
					varcharValue,
					integerValue.(int32),
					fields["suffix"].(string),
				)
				if err = output.Set(row, result); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func TestScalarUDFMixedVectorAndValueAccess(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	varcharInfo := mustTypeInfo(t, TYPE_VARCHAR)
	integerInfo := mustTypeInfo(t, TYPE_INTEGER)
	suffixEntry, err := NewStructEntry(varcharInfo, "suffix")
	require.NoError(t, err)
	structInfo, err := NewStructInfo(suffixEntry)
	require.NoError(t, err)

	udf := &mixedVectorAccessUDF{
		varcharInfo: varcharInfo,
		integerInfo: integerInfo,
		structInfo:  structInfo,
	}
	require.NoError(t, RegisterScalarUDF(conn, "mixed_vector_access", udf))

	const varcharValue = "a VARCHAR value longer than twelve bytes"
	var result string
	err = conn.QueryRowContext(ctx, `
		SELECT mixed_vector_access(
			?,
			7::INTEGER,
			{'suffix': 'legacy struct value'}
		)
	`, varcharValue).Scan(&result)
	require.NoError(t, err)
	require.Equal(
		t,
		"a VARCHAR value longer than twelve bytes:7:legacy struct value",
		result,
	)
}

func stringDataAddress(value string) uintptr {
	return uintptr(unsafe.Pointer(unsafe.StringData(value)))
}
