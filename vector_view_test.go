package duckdb

import (
	"context"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

type namedVarchar string

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

func mustVectorViewTypeInfo(t testing.TB, typ Type) TypeInfo {
	t.Helper()
	info, err := NewTypeInfo(typ)
	require.NoError(t, err)
	return info
}

func TestDuckDBStringTViewLayout(t *testing.T) {
	require.Equal(t, uintptr(16), unsafe.Sizeof(mapping.StringT{}))
}

func TestVarcharVectorView(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustVectorViewTypeInfo(t, TYPE_VARCHAR))
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

	view, err := getVectorView[string](chunk, 0)
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

	namedView, err := getVectorView[namedVarchar](chunk, 0)
	require.NoError(t, err)
	namedValue, valid, err := namedView.GetValue(1)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, namedVarchar(values[1]), namedValue)
}

func TestVarcharVectorViewValidation(t *testing.T) {
	intInfo := mustVectorViewTypeInfo(t, TYPE_INTEGER)
	stringInfo := mustVectorViewTypeInfo(t, TYPE_VARCHAR)
	chunk := newVectorViewTestChunk(t, intInfo, stringInfo)
	require.NoError(t, chunk.SetSize(1))

	_, err := getVectorView[string](chunk, 0)
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be read as Go string")

	_, err = getVectorView[string](chunk, -1)
	require.ErrorIs(t, err, errAPI)

	_, err = getVectorView[string](chunk, 2)
	require.ErrorIs(t, err, errAPI)

	var nilChunk *DataChunk
	_, err = getVectorView[string](nilChunk, 0)
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errNilDataChunk)

	var zeroView VectorView[string]
	_, _, err = zeroView.GetValue(0)
	require.ErrorIs(t, err, errUninitializedVectorView)
	var nilState *ChunkIteratorState
	_, err = GetInputVectorView[string](nilState, 0)
	require.ErrorIs(t, err, errUninitializedChunkIterator)

	view, err := getVectorView[string](chunk, 1)
	require.NoError(t, err)
	_, _, err = view.GetValue(-1)
	require.ErrorContains(t, err, rowIndexErrMsg)
	_, _, err = view.GetValue(view.Len())
	require.ErrorContains(t, err, rowIndexErrMsg)

	chunk.projection = []int{1}
	projected, err := getVectorView[string](chunk, 0)
	require.NoError(t, err)
	require.Equal(t, 1, projected.Len())
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

	_, err := getVectorView[string](chunk, 0)
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB JSON cannot be read as Go string")

	owned, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"answer": float64(42)}, owned)
}

type vectorViewIdentityUDF struct {
	info                TypeInfo
	specialNullHandling bool
}

func (udf *vectorViewIdentityUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos:      []TypeInfo{udf.info},
		ResultTypeInfo:      udf.info,
		SpecialNullHandling: udf.specialNullHandling,
	}
}

func (*vectorViewIdentityUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		ChunkContextExecutor: func(_ context.Context, chunk *ChunkIteratorState) error {
			input, err := GetInputVectorView[string](chunk, 0)
			if err != nil {
				return err
			}
			output, err := GetResultVectorWriter[string](chunk)
			if err != nil {
				return err
			}
			for row := range input.Len() {
				value, valid, err := input.GetValue(row)
				if err != nil {
					return err
				}
				if !valid {
					value = "handled NULL"
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

	udf := &vectorViewIdentityUDF{info: mustVectorViewTypeInfo(t, TYPE_VARCHAR)}
	require.NoError(t, RegisterScalarUDF(conn, "vector_view_identity", udf))

	rows, err := db.Query(`
		SELECT vector_view_identity(value)
		FROM (VALUES ('short'), ('a value longer than twelve bytes'), (NULL)) t(value)
	`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)

	expected := []*string{
		ptr("short"),
		ptr("a value longer than twelve bytes"),
		nil,
	}
	var actual []*string
	for rows.Next() {
		var value *string
		require.NoError(t, rows.Scan(&value))
		actual = append(actual, value)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, actual)
}

func TestVectorViewScalarUDFMultipleChunks(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	udf := &vectorViewIdentityUDF{info: mustVectorViewTypeInfo(t, TYPE_VARCHAR)}
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

	udf := &vectorViewIdentityUDF{
		info:                mustVectorViewTypeInfo(t, TYPE_VARCHAR),
		specialNullHandling: true,
	}
	require.NoError(t, RegisterScalarUDF(conn, "vector_view_identity_special", udf))

	var result string
	require.NoError(
		t,
		db.QueryRow(`SELECT vector_view_identity_special(NULL)`).Scan(&result),
	)
	require.Equal(t, "handled NULL", result)
}

func ptr[T any](value T) *T {
	return &value
}

func stringDataAddress(value string) uintptr {
	return uintptr(unsafe.Pointer(unsafe.StringData(value)))
}
