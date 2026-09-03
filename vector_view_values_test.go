package duckdb

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestDecimalVectorViewValues(t *testing.T) {
	tests := []struct {
		name     string
		width    uint8
		scale    uint8
		unscaled string
	}{
		{name: "SMALLINT", width: 4, scale: 2, unscaled: "-1234"},
		{name: "INTEGER", width: 9, scale: 2, unscaled: "123456789"},
		{name: "BIGINT", width: 18, scale: 4, unscaled: "-123456789012345678"},
		{name: "HUGEINT", width: 38, scale: 8, unscaled: "12345678901234567890123456789012345678"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := NewDecimalInfo(tt.width, tt.scale)
			require.NoError(t, err)
			chunk := newVectorViewTestChunk(t, info)
			integer, ok := new(big.Int).SetString(tt.unscaled, 10)
			require.True(t, ok)
			require.NoError(t, chunk.SetValue(0, 0, Decimal{
				Width: tt.width,
				Scale: tt.scale,
				Value: integer,
			}))
			require.NoError(t, chunk.SetValue(0, 1, nil))
			require.NoError(t, chunk.SetSize(2))

			view, err := GetVectorView[DecimalValue](mustGetVector(t, chunk, 0))
			require.NoError(t, err)
			requireVectorViewReadDoesNotAllocate(t, view)
			value, valid, err := view.Get(0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Equal(t, tt.width, value.Width)
			require.Equal(t, tt.scale, value.Scale)
			require.Equal(t, integer, int128BigInt(value.Unscaled))

			value, valid, err = view.Get(1)
			require.NoError(t, err)
			require.False(t, valid)
			require.Equal(t, DecimalValue{}, value)
		})
	}
}

func TestScalarVectorViewValues(t *testing.T) {
	t.Run("HUGEINT", func(t *testing.T) {
		chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_HUGEINT))
		expected := new(big.Int).Lsh(big.NewInt(1), 100)
		expected.Neg(expected)
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[Int128](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, expected, int128BigInt(value))
	})

	t.Run("UHUGEINT", func(t *testing.T) {
		chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_UHUGEINT))
		expected := new(big.Int).Lsh(big.NewInt(1), 100)
		expected.Add(expected, big.NewInt(42))
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[Uint128](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, expected, uint128BigInt(value))
	})

	t.Run("UUID", func(t *testing.T) {
		chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_UUID))
		expected := UUID{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[UUID](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, expected, value)
	})

	t.Run("INTERVAL", func(t *testing.T) {
		chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTERVAL))
		expected := Interval{Months: 14, Days: -3, Micros: 987654321}
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[Interval](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, expected, value)
	})
}

func TestTemporalVectorViewValues(t *testing.T) {
	tests := []struct {
		name     string
		typeID   Type
		input    time.Time
		expected time.Time
	}{
		{
			name: "DATE", typeID: TYPE_DATE,
			input:    time.Date(2026, time.September, 2, 13, 14, 15, 0, time.UTC),
			expected: time.Date(2026, time.September, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "TIME", typeID: TYPE_TIME,
			input:    time.Date(2026, time.September, 2, 13, 14, 15, 123456000, time.UTC),
			expected: time.Date(1, time.January, 1, 13, 14, 15, 123456000, time.UTC),
		},
		{
			name: "TIMESTAMP_NS", typeID: TYPE_TIMESTAMP_NS,
			input:    time.Date(2026, time.September, 2, 13, 14, 15, 123456789, time.UTC),
			expected: time.Date(2026, time.September, 2, 13, 14, 15, 123456789, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := newVectorViewTestChunk(t, mustTypeInfo(t, tt.typeID))
			require.NoError(t, chunk.SetValue(0, 0, tt.input))
			require.NoError(t, chunk.SetSize(1))

			view, err := GetVectorView[time.Time](mustGetVector(t, chunk, 0))
			require.NoError(t, err)
			requireVectorViewReadDoesNotAllocate(t, view)
			value, valid, err := view.Get(0)
			require.NoError(t, err)
			require.True(t, valid)
			require.True(t, tt.expected.Equal(value), "expected %s, got %s", tt.expected, value)
		})
	}
}

func TestTimeTZVectorViewValue(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_TIME_TZ))
	zone := time.FixedZone("", 5*60*60+30*60)
	input := time.Date(1, time.January, 1, 13, 14, 15, 123456000, zone)
	require.NoError(t, chunk.SetValue(0, 0, input))
	require.NoError(t, chunk.SetSize(1))

	view, err := GetVectorView[TimeTZValue](mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	requireVectorViewReadDoesNotAllocate(t, view)
	value, valid, err := view.Get(0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, int64((13*60*60+14*60+15)*1_000_000+123456), value.Micros)
	require.Equal(t, int32(5*60*60+30*60), value.OffsetSeconds)
}

func TestVariableWidthScalarVectorViewValues(t *testing.T) {
	t.Run("ENUM", func(t *testing.T) {
		info, err := NewEnumInfo("red", "green", "blue")
		require.NoError(t, err)
		chunk := newVectorViewTestChunk(t, info)
		require.NoError(t, chunk.SetValue(0, 0, "green"))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[string](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, "green", value)
	})

	for _, typeID := range []Type{TYPE_BLOB, TYPE_GEOMETRY} {
		t.Run(typeName(typeID), func(t *testing.T) {
			chunk := newVectorViewTestChunk(t, mustTypeInfo(t, typeID))
			expected := []byte{0x00, 0xff, 0x42, 0x00, 0x7f}
			require.NoError(t, chunk.SetValue(0, 0, expected))
			require.NoError(t, chunk.SetSize(1))

			view, err := GetVectorView[[]byte](mustGetVector(t, chunk, 0))
			require.NoError(t, err)
			requireVectorViewReadDoesNotAllocate(t, view)
			value, valid, err := view.Get(0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Equal(t, expected, value)
		})
	}

	t.Run("BIT", func(t *testing.T) {
		chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_BIT))
		expected, err := NewBitFromString("101001011")
		require.NoError(t, err)
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[Bit](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.Equal(t, expected.String(), value.String())
	})

	t.Run("JSON", func(t *testing.T) {
		logicalType := mapping.CreateLogicalType(TYPE_VARCHAR)
		mapping.LogicalTypeSetAlias(logicalType, aliasJSON)
		defer mapping.DestroyLogicalType(&logicalType)

		chunk := &DataChunk{}
		require.NoError(t, chunk.initFromTypes([]mapping.LogicalType{logicalType}, true))
		t.Cleanup(chunk.close)
		expected := json.RawMessage(`{"answer":42}`)
		require.NoError(t, chunk.SetValue(0, 0, expected))
		require.NoError(t, chunk.SetSize(1))

		view, err := GetVectorView[RawJSON](mustGetVector(t, chunk, 0))
		require.NoError(t, err)
		requireVectorViewReadDoesNotAllocate(t, view)
		value, valid, err := view.Get(0)
		require.NoError(t, err)
		require.True(t, valid)
		require.JSONEq(t, string(expected), value.String())
	})
}

func TestScalarVectorViewValidation(t *testing.T) {
	integerChunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTEGER))
	require.NoError(t, integerChunk.SetSize(1))

	_, err := GetVectorView[DecimalValue](mustGetVector(t, integerChunk, 0))
	require.ErrorContains(t, err, "cannot be read as Go duckdb.DecimalValue")
	_, err = GetVectorView[Int128](mustGetVector(t, integerChunk, 0))
	require.ErrorContains(t, err, "cannot be read as Go duckdb.Int128")
	_, err = GetVectorView[[]byte](mustGetVector(t, integerChunk, 0))
	require.ErrorContains(t, err, "cannot be read as Go slice")

	varcharChunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_VARCHAR))
	require.NoError(t, varcharChunk.SetSize(1))
	_, err = GetVectorView[RawJSON](mustGetVector(t, varcharChunk, 0))
	require.ErrorContains(t, err, "cannot be read as Go duckdb.RawJSON")
}

func TestScalarVectorViewReadsDoNotAllocate(t *testing.T) {
	info, err := NewDecimalInfo(18, 2)
	require.NoError(t, err)
	chunk := newVectorViewTestChunk(t, info)
	require.NoError(t, chunk.SetValue(0, 0, Decimal{Width: 18, Scale: 2, Value: big.NewInt(12345)}))
	require.NoError(t, chunk.SetSize(1))
	view, err := GetVectorView[DecimalValue](mustGetVector(t, chunk, 0))
	require.NoError(t, err)

	var value DecimalValue
	var valid bool
	allocations := testing.AllocsPerRun(1000, func() {
		value, valid, err = view.Get(0)
	})
	require.NoError(t, err)
	require.True(t, valid)
	unscaled, fits := value.UnscaledInt64()
	require.True(t, fits)
	require.Equal(t, int64(12345), unscaled)
	require.Zero(t, allocations)
}

func requireVectorViewReadDoesNotAllocate[T vectorViewValue](t testing.TB, view VectorView[T]) {
	t.Helper()
	var value T
	var valid bool
	var err error
	allocations := testing.AllocsPerRun(1000, func() {
		value, valid, err = view.Get(0)
	})
	require.NoError(t, err)
	require.True(t, valid)
	require.Zero(t, allocations)
	_ = value
}

func int128BigInt(value Int128) *big.Int {
	result := big.NewInt(value.Upper)
	result.Lsh(result, 64)
	result.Add(result, new(big.Int).SetUint64(value.Lower))
	return result
}

func uint128BigInt(value Uint128) *big.Int {
	result := new(big.Int).SetUint64(value.Upper)
	result.Lsh(result, 64)
	result.Add(result, new(big.Int).SetUint64(value.Lower))
	return result
}
