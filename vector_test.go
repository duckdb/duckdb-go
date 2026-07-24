package duckdb

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestSetGetPrimitive(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(int32(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []int32{-100, 0, 42, 1337, 2147483647}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[int32](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})

	t.Run("float64", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(float64(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []float64{-3.14, 0.0, 2.718, 1e10, -1e-10}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[float64](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})

	t.Run("bool", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(bool(false)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		setPrimitive(vec, 0, true)
		setPrimitive(vec, 1, false)
		setPrimitive(vec, 2, true)

		require.True(t, getPrimitive[bool](vec, 0))
		require.False(t, getPrimitive[bool](vec, 1))
		require.True(t, getPrimitive[bool](vec, 2))
	})

	t.Run("uint64", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(uint64(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []uint64{0, 1, 42, 18446744073709551615}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[uint64](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})
}

func TestSetGetPrimitiveLargeIndex(t *testing.T) {
	data := make([]byte, 10000*int(unsafe.Sizeof(int32(0))))
	vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

	testCases := []struct {
		idx mapping.IdxT
		val int32
	}{
		{0, 100},
		{100, 200},
		{1000, 300},
		{5000, 400},
		{9999, 500},
	}

	for _, tc := range testCases {
		setPrimitive(vec, tc.idx, tc.val)
		got := getPrimitive[int32](vec, tc.idx)
		require.Equal(t, tc.val, got, "value at index %d", tc.idx)
	}
}

func TestSetChunkValueArray(t *testing.T) {
	childInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	arrayInfo, err := NewArrayInfo(childInfo, 3)
	require.NoError(t, err)

	logicalType := arrayInfo.logicalType()
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	defer chunk.close()

	require.NoError(t, SetChunkValue(chunk, 0, 0, [3]int32{1, 2, 3}))
	got, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2), int32(3)}, got)
}

func TestSetChunkValueJSON(t *testing.T) {
	logicalType := mapping.CreateLogicalType(TYPE_VARCHAR)
	mapping.LogicalTypeSetAlias(logicalType, aliasJSON)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	defer chunk.close()

	// Strings are JSON values, not pre-serialized JSON documents.
	require.NoError(t, SetChunkValue(chunk, 0, 0, `{"a":1}`))
	got, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, got)

	require.NoError(t, SetChunkValue(chunk, 0, 1, map[string]any{"a": 1}))
	got, err = chunk.GetValue(0, 1)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": float64(1)}, got)

	require.NoError(t, SetChunkValue(chunk, 0, 2, json.RawMessage(`{"a":2}`)))
	got, err = chunk.GetValue(0, 2)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": float64(2)}, got)
}

func TestSetChunkValueNilSetsNull(t *testing.T) {
	logicalType := mapping.CreateLogicalType(TYPE_INTEGER)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	defer chunk.close()

	require.NoError(t, SetChunkValue(chunk, 0, 0, int32(42)))
	var value any
	require.NoError(t, SetChunkValue(chunk, 0, 0, value))
	got, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestDataChunkGetValueBubblesGetterErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*vector)
	}{
		{
			name: "decimal",
			setup: func(vec *vector) {
				vec.Type = TYPE_DECIMAL
				vec.internalType = TYPE_VARCHAR
				vec.getFn = func(vec *vector, rowIdx mapping.IdxT) (any, error) {
					return vec.getDecimal(rowIdx)
				}
			},
		},
		{
			name: "enum",
			setup: func(vec *vector) {
				vec.Type = TYPE_ENUM
				vec.internalType = TYPE_VARCHAR
				vec.getFn = func(vec *vector, rowIdx mapping.IdxT) (any, error) {
					return vec.getEnum(rowIdx)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var column vector
			tc.setup(&column)
			chunk := DataChunk{columns: []vector{column}}

			var err error
			require.NotPanics(t, func() {
				_, err = chunk.GetValue(0, 0)
			})
			require.ErrorIs(t, err, errAPI)
			require.ErrorContains(t, err, unsupportedTypeErrMsg)
		})
	}
}

func TestDataChunkGetValueReturnsJSONDecodeError(t *testing.T) {
	logicalType := mapping.CreateLogicalType(TYPE_VARCHAR)
	mapping.LogicalTypeSetAlias(logicalType, aliasJSON)
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	defer chunk.close()

	require.NoError(t, setBytes(&chunk.columns[0], 0, "invalid"))
	got, err := chunk.GetValue(0, 0)
	require.Nil(t, got)
	require.ErrorIs(t, err, errAPI)
	var syntaxErr *json.SyntaxError
	require.ErrorAs(t, err, &syntaxErr)
}

func TestRowsNextBubblesGetterErrors(t *testing.T) {
	var column vector
	column.Type = TYPE_DECIMAL
	column.internalType = TYPE_VARCHAR
	column.getFn = func(vec *vector, rowIdx mapping.IdxT) (any, error) {
		return vec.getDecimal(rowIdx)
	}

	r := rows{
		chunk: DataChunk{
			columns: []vector{column},
			size:    1,
		},
	}
	dst := make([]driver.Value, 1)

	var err error
	require.NotPanics(t, func() {
		err = r.Next(dst)
	})
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, unsupportedTypeErrMsg)
}
