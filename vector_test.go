package duckdb

import (
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"testing"
	"time"
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

// newTestChunk builds a single-column data chunk from logicalType, taking
// ownership of it. The chunk is closed when the test ends.
func newTestChunk(t testing.TB, logicalType mapping.LogicalType) DataChunk {
	t.Helper()
	defer mapping.DestroyLogicalType(&logicalType)

	var chunk DataChunk
	require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
	t.Cleanup(chunk.close)
	return chunk
}

// newJSONLogicalType builds a VARCHAR logical type carrying DuckDB's JSON
// alias. There is no TypeInfo for it, so the alias has to be set by hand.
func newJSONLogicalType() mapping.LogicalType {
	logicalType := mapping.CreateLogicalType(TYPE_VARCHAR)
	mapping.LogicalTypeSetAlias(logicalType, aliasJSON)
	return logicalType
}

// newIntStructLogicalType builds a STRUCT with a single INTEGER field "value".
func newIntStructLogicalType(t *testing.T) mapping.LogicalType {
	t.Helper()

	entry, err := NewStructEntry(mustTypeInfo(t, TYPE_INTEGER), "value")
	require.NoError(t, err)
	info, err := NewStructInfo(entry)
	require.NoError(t, err)
	return info.logicalType()
}

// newIntUnionLogicalType builds a UNION with a single INTEGER member "value".
func newIntUnionLogicalType(t *testing.T) mapping.LogicalType {
	t.Helper()

	info, err := NewUnionInfo([]TypeInfo{mustTypeInfo(t, TYPE_INTEGER)}, []string{"value"})
	require.NoError(t, err)
	return info.logicalType()
}

// newTypeTestChunk builds a single-column data chunk of the given type.
func newTypeTestChunk(t testing.TB, typ Type) DataChunk {
	t.Helper()
	return newTestChunk(t, mapping.CreateLogicalType(typ))
}

func TestSetChunkValueArray(t *testing.T) {
	arrayInfo, err := NewArrayInfo(mustTypeInfo(t, TYPE_INTEGER), 3)
	require.NoError(t, err)
	chunk := newTestChunk(t, arrayInfo.logicalType())

	require.NoError(t, SetChunkValue(chunk, 0, 0, [3]int32{1, 2, 3}))
	got, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2), int32(3)}, got)

	require.NoError(t, SetChunkValue(chunk, 0, 1, []any{int32(4), int32(5), int32(6)}))
	got, err = chunk.GetValue(0, 1)
	require.NoError(t, err)
	require.Equal(t, []any{int32(4), int32(5), int32(6)}, got)

	err = SetChunkValue(chunk, 0, 2, []any{int32(7), int32(8)})
	require.Error(t, err)
}

func TestSetChunkValueJSON(t *testing.T) {
	chunk := newTestChunk(t, newJSONLogicalType())

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

	// Byte slices are JSON byte values and therefore use base64 encoding.
	require.NoError(t, SetChunkValue(chunk, 0, 3, []byte(`{"a":3}`)))
	got, err = chunk.GetValue(0, 3)
	require.NoError(t, err)
	require.Equal(t, "eyJhIjozfQ==", got)
}

func TestSetChunkValueNumericConversionsPreserveAdjacentRows(t *testing.T) {
	tests := []struct {
		name  string
		typ   Type
		write func(DataChunk, int) error
		want  any
	}{
		{"TINYINT", TYPE_TINYINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, int16(11))
		}, int8(11)},
		{"SMALLINT", TYPE_SMALLINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, int32(22))
		}, int16(22)},
		{"INTEGER", TYPE_INTEGER, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, int64(33))
		}, int32(33)},
		{"BIGINT", TYPE_BIGINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, int(44))
		}, int64(44)},
		{"UTINYINT", TYPE_UTINYINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, uint16(55))
		}, uint8(55)},
		{"USMALLINT", TYPE_USMALLINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, uint32(66))
		}, uint16(66)},
		{"UINTEGER", TYPE_UINTEGER, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, uint64(77))
		}, uint32(77)},
		{"UBIGINT", TYPE_UBIGINT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, uint(88))
		}, uint64(88)},
		{"FLOAT", TYPE_FLOAT, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, float64(99.5))
		}, float32(99.5)},
		{"DOUBLE", TYPE_DOUBLE, func(chunk DataChunk, row int) error {
			return SetChunkValue(chunk, 0, row, float32(110.5))
		}, float64(110.5)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := newTypeTestChunk(t, tc.typ)

			require.NoError(t, chunk.SetValue(0, 0, tc.want))
			require.NoError(t, chunk.SetValue(0, 2, tc.want))
			require.NoError(t, tc.write(chunk, 1))

			for row := range 3 {
				got, err := chunk.GetValue(0, row)
				require.NoError(t, err)
				require.Equal(t, tc.want, got, "row %d", row)
			}
		})
	}
}

func testSetChunkValueAgreesWithSetValue[T any](
	t *testing.T,
	logicalType mapping.LogicalType,
	value T,
) {
	t.Helper()
	chunk := newTestChunk(t, logicalType)

	require.NoError(t, SetChunkValue(chunk, 0, 0, value))
	require.NoError(t, chunk.SetValue(0, 1, value))

	typedValue, err := chunk.GetValue(0, 0)
	require.NoError(t, err)
	interfaceValue, err := chunk.GetValue(0, 1)
	require.NoError(t, err)
	require.Equal(t, interfaceValue, typedValue)
}

func TestSetChunkValueCanonicalSettersAgreeWithSetValue(t *testing.T) {
	timestamp := time.Date(2025, time.January, 2, 3, 4, 5, 6000, time.UTC)
	bit, err := NewBitFromString("10101")
	require.NoError(t, err)

	t.Run("BOOLEAN", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_BOOLEAN), true)
	})
	t.Run("numeric", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_BIGINT), int64(42))
	})
	t.Run("TIMESTAMP", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_TIMESTAMP), timestamp)
	})
	t.Run("DATE", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_DATE), timestamp)
	})
	t.Run("TIME", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_TIME), timestamp)
	})
	t.Run("INTERVAL", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_INTERVAL), Interval{Days: 2})
	})
	t.Run("VARCHAR", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_VARCHAR), "hello")
	})
	t.Run("BLOB", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_BLOB), []byte("hello"))
	})
	t.Run("BIT", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_BIT), bit)
	})
	t.Run("JSON", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, newJSONLogicalType(), `{"a":1}`)
	})
	t.Run("ENUM", func(t *testing.T) {
		info, err := NewEnumInfo("one", "two")
		require.NoError(t, err)
		testSetChunkValueAgreesWithSetValue(t, info.logicalType(), "two")
	})
	t.Run("LIST", func(t *testing.T) {
		info, err := NewListInfo(mustTypeInfo(t, TYPE_INTEGER))
		require.NoError(t, err)
		testSetChunkValueAgreesWithSetValue(t, info.logicalType(), []any{int32(1)})
	})
	t.Run("STRUCT", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, newIntStructLogicalType(t), map[string]any{"value": int32(1)})
	})
	t.Run("MAP", func(t *testing.T) {
		info, err := NewMapInfo(mustTypeInfo(t, TYPE_VARCHAR), mustTypeInfo(t, TYPE_INTEGER))
		require.NoError(t, err)
		orderedMap := OrderedMap{keys: []any{"one"}, values: []any{int32(1)}}
		testSetChunkValueAgreesWithSetValue(t, info.logicalType(), orderedMap)
	})
	t.Run("ARRAY", func(t *testing.T) {
		info, err := NewArrayInfo(mustTypeInfo(t, TYPE_INTEGER), 2)
		require.NoError(t, err)
		testSetChunkValueAgreesWithSetValue(t, info.logicalType(), []any{int32(1), int32(2)})
	})
	t.Run("UNION", func(t *testing.T) {
		union := Union{Tag: "value", Value: int32(1)}
		testSetChunkValueAgreesWithSetValue(t, newIntUnionLogicalType(t), union)
	})
	t.Run("UUID", func(t *testing.T) {
		testSetChunkValueAgreesWithSetValue(t, mapping.CreateLogicalType(TYPE_UUID), UUID{1})
	})
}

// TestSetChunkValueAllocations pins the property the generic write API exists
// for: SetChunkValue keeps the caller's concrete type, so the compiler
// instantiates the setter directly instead of boxing val. Every write below is
// allocation-free.
func TestSetChunkValueAllocations(t *testing.T) {
	// Built up front so that only the write itself is measured.
	byteValues := [][]byte{[]byte("one"), []byte("two")}
	stringValues := []string{"one", "two"}

	tests := []struct {
		name       string
		typ        Type
		write      func(DataChunk, int) error
		wantAllocs float64
	}{
		{"BIGINT_from_int64", TYPE_BIGINT, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, int64(i))
		}, 0},
		{"BIGINT_from_int", TYPE_BIGINT, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, i)
		}, 0},
		{"BOOLEAN_from_bool", TYPE_BOOLEAN, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, i%2 == 0)
		}, 0},
		{"TIMESTAMP_from_time", TYPE_TIMESTAMP, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, time.Unix(int64(i), 0))
		}, 0},
		{"INTERVAL_from_Interval", TYPE_INTERVAL, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, Interval{Days: int32(i)})
		}, 0},
		{"UUID_from_UUID", TYPE_UUID, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, UUID{byte(i)})
		}, 0},
		// Strings and byte slices hand their backing array straight to DuckDB.
		{"VARCHAR_from_bytes", TYPE_VARCHAR, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, byteValues[i%len(byteValues)])
		}, 0},
		{"VARCHAR_from_string", TYPE_VARCHAR, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, stringValues[i%len(stringValues)])
		}, 0},
		{"BLOB_from_string", TYPE_BLOB, func(chunk DataChunk, i int) error {
			return SetChunkValue(chunk, 0, 0, stringValues[i%len(stringValues)])
		}, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := newTypeTestChunk(t, tc.typ)

			// Vary the value so boxing cannot reuse a cached small-integer box.
			i := 1000
			var setErr error
			allocs := testing.AllocsPerRun(100, func() {
				i++
				setErr = tc.write(chunk, i)
			})
			require.NoError(t, setErr)
			require.Equal(t, tc.wantAllocs, allocs)
		})
	}
}

func TestSetChunkValueNilSetsNull(t *testing.T) {
	tests := []struct {
		name        string
		logicalType func(*testing.T) mapping.LogicalType
		value       any
	}{
		{"INTEGER", func(*testing.T) mapping.LogicalType {
			return mapping.CreateLogicalType(TYPE_INTEGER)
		}, int32(42)},
		{"LIST", func(t *testing.T) mapping.LogicalType {
			info, err := NewListInfo(mustTypeInfo(t, TYPE_INTEGER))
			require.NoError(t, err)
			return info.logicalType()
		}, []any{int32(42)}},
		{"STRUCT", newIntStructLogicalType, map[string]any{"value": int32(42)}},
		{"UNION", newIntUnionLogicalType, Union{Tag: "value", Value: int32(42)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := newTestChunk(t, tc.logicalType(t))

			require.NoError(t, SetChunkValue(chunk, 0, 0, tc.value))
			var null any
			require.NoError(t, SetChunkValue(chunk, 0, 0, null))
			got, err := chunk.GetValue(0, 0)
			require.NoError(t, err)
			require.Nil(t, got)
		})
	}
}

// TestSetChunkValueTypedNilSetsNull covers the typed-nil pointers the write API
// accepts. They normalize to SQL NULL for every column type, not only for the
// column type that matches the pointer.
func TestSetChunkValueTypedNilSetsNull(t *testing.T) {
	var (
		nilBigInt   *big.Int
		nilTime     *time.Time
		nilInterval *Interval
		nilBit      *Bit
		nilUUID     *UUID
	)

	tests := []struct {
		name  string
		typ   Type
		value any
	}{
		{"HUGEINT_from_nil_bigint", TYPE_HUGEINT, nilBigInt},
		{"UHUGEINT_from_nil_bigint", TYPE_UHUGEINT, nilBigInt},
		{"BIGNUM_from_nil_bigint", TYPE_BIGNUM, nilBigInt},
		{"TIMESTAMP_from_nil_time", TYPE_TIMESTAMP, nilTime},
		{"DATE_from_nil_time", TYPE_DATE, nilTime},
		{"INTERVAL_from_nil_interval", TYPE_INTERVAL, nilInterval},
		{"BIT_from_nil_bit", TYPE_BIT, nilBit},
		{"UUID_from_nil_uuid", TYPE_UUID, nilUUID},
		// A typed nil is a NULL write regardless of the column it targets.
		{"VARCHAR_from_nil_time", TYPE_VARCHAR, nilTime},
		{"BIGINT_from_nil_bigint", TYPE_BIGINT, nilBigInt},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := newTypeTestChunk(t, tc.typ)

			require.NoError(t, SetChunkValue(chunk, 0, 0, tc.value))
			got, err := chunk.GetValue(0, 0)
			require.NoError(t, err)
			require.Nil(t, got)
		})
	}
}

// TestSetChunkValueSQLNullRejectsWrites covers the one column type that has no
// writable storage. Even a NULL write is an error.
func TestSetChunkValueSQLNullRejectsWrites(t *testing.T) {
	chunk := newTypeTestChunk(t, TYPE_SQLNULL)

	require.ErrorIs(t, SetChunkValue(chunk, 0, 0, int32(42)), errSetSQLNULLValue)
	var null any
	require.ErrorIs(t, SetChunkValue(chunk, 0, 0, null), errSetSQLNULLValue)
	require.ErrorIs(t, chunk.SetValue(0, 0, null), errSetSQLNULLValue)
}

// TestSetChunkValueErrorReportsPosition covers both write entry points
// reporting the same column and row context on failure.
func TestSetChunkValueErrorReportsPosition(t *testing.T) {
	chunk := newTypeTestChunk(t, TYPE_BIGINT)
	const input = "sensitive input"

	genericErr := SetChunkValue(chunk, 0, 3, input)
	require.ErrorContains(t, genericErr, setValueErrMsg)
	require.ErrorContains(t, genericErr, "at row 3, col 0")
	require.NotContains(t, genericErr.Error(), input)

	anyErr := chunk.SetValue(0, 3, input)
	require.ErrorContains(t, anyErr, setValueErrMsg)
	require.ErrorContains(t, anyErr, "at row 3, col 0")
	require.NotContains(t, anyErr.Error(), input)
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
