package duckdb

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func nestedDecimalStructInfo(t testing.TB) TypeInfo {
	t.Helper()

	smallInfo, err := NewDecimalInfo(4, 2)
	require.NoError(t, err)
	largeInfo, err := NewDecimalInfo(38, 2)
	require.NoError(t, err)

	smallEntry, err := NewStructEntry(smallInfo, "small")
	require.NoError(t, err)
	largeEntry, err := NewStructEntry(largeInfo, "large")
	require.NoError(t, err)
	amountsInfo, err := NewStructInfo(smallEntry, largeEntry)
	require.NoError(t, err)

	amountsEntry, err := NewStructEntry(amountsInfo, "amounts")
	require.NoError(t, err)
	info, err := NewStructInfo(amountsEntry)
	require.NoError(t, err)
	return info
}

func TestNestedStructViewDecimalFields(t *testing.T) {
	info := nestedDecimalStructInfo(t)
	chunk := newVectorViewTestChunk(t, info)

	largeUnscaled, ok := new(big.Int).SetString("12345678901234567890123456789012345678", 10)
	require.True(t, ok)
	require.NoError(t, chunk.SetValue(0, 0, map[string]any{
		"amounts": map[string]any{
			"small": Decimal{Width: 4, Scale: 2, Value: big.NewInt(1234)},
			"large": Decimal{Width: 38, Scale: 2, Value: largeUnscaled},
		},
	}))
	require.NoError(t, chunk.SetValue(0, 1, nil))
	require.NoError(t, chunk.SetValue(0, 2, map[string]any{"amounts": nil}))
	require.NoError(t, chunk.SetSize(3))

	root, err := GetStructView(mustGetVector(t, chunk, 0))
	require.NoError(t, err)
	require.Equal(t, 3, root.Len())

	amountsVector, err := root.Field("amounts")
	require.NoError(t, err)
	amounts, err := GetStructView(amountsVector)
	require.NoError(t, err)

	smallVector, err := root.Field("amounts", "small")
	require.NoError(t, err)
	largeVector, err := root.Field("amounts", "large")
	require.NoError(t, err)
	small, err := GetVectorView[DecimalValue](smallVector)
	require.NoError(t, err)
	large, err := GetVectorView[DecimalValue](largeVector)
	require.NoError(t, err)

	value, valid, err := small.Get(0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, uint8(4), value.Width)
	require.Equal(t, uint8(2), value.Scale)
	unscaled, fits := value.UnscaledInt64()
	require.True(t, fits)
	require.Equal(t, int64(1234), unscaled)

	value, valid, err = large.Get(0)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, uint8(38), value.Width)
	require.Equal(t, uint8(2), value.Scale)
	require.Equal(t, largeUnscaled.Uint64(), value.Unscaled.Lower)

	// A child vector can be valid independently of an enclosing STRUCT. The
	// field view must still respect both enclosing validity masks.
	mapping.ValiditySetRowValidity(amountsVector.v.maskPtr, 1, true)
	mapping.ValiditySetRowValidity(smallVector.v.maskPtr, 1, true)
	mapping.ValiditySetRowValidity(smallVector.v.maskPtr, 2, true)

	_, valid, err = small.Get(1)
	require.NoError(t, err)
	require.False(t, valid)
	_, valid, err = small.Get(2)
	require.NoError(t, err)
	require.False(t, valid)

	valid, err = amounts.IsValid(1)
	require.NoError(t, err)
	require.False(t, valid)
	valid, err = amounts.IsValid(2)
	require.NoError(t, err)
	require.False(t, valid)
	valid, err = root.IsValid(2)
	require.NoError(t, err)
	require.True(t, valid)

	allocs := testing.AllocsPerRun(100, func() {
		_, _, getErr := large.Get(0)
		require.NoError(t, getErr)
	})
	require.Zero(t, allocs)
}

func TestStructViewValidation(t *testing.T) {
	var zero StructView
	_, err := zero.IsValid(0)
	require.ErrorIs(t, err, errUninitializedStructView)
	_, err = zero.Field("value")
	require.ErrorIs(t, err, errUninitializedStructView)

	_, err = GetStructView(Vector{})
	require.ErrorIs(t, err, errUninitializedStructView)

	integerChunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTEGER))
	_, err = GetStructView(mustGetVector(t, integerChunk, 0))
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be read as STRUCT")

	chunk := newVectorViewTestChunk(t, nestedDecimalStructInfo(t))
	require.NoError(t, chunk.SetSize(1))
	view, err := GetStructView(mustGetVector(t, chunk, 0))
	require.NoError(t, err)

	_, err = view.Field()
	require.ErrorIs(t, err, errEmptyStructFieldPath)
	_, err = view.Field("missing")
	require.ErrorContains(t, err, "STRUCT field not found: missing")
	_, err = view.Field("amounts", "small", "invalid")
	require.ErrorContains(t, err, "DuckDB type is DECIMAL, not STRUCT")
	_, err = view.IsValid(-1)
	require.ErrorContains(t, err, rowIndexErrMsg)
	_, err = view.IsValid(view.Len())
	require.ErrorContains(t, err, rowIndexErrMsg)
}

type nestedDecimalStructUDF struct {
	inputInfo  TypeInfo
	resultInfo TypeInfo
}

func (udf *nestedDecimalStructUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{udf.inputInfo},
		ResultTypeInfo: udf.resultInfo,
	}
}

func (*nestedDecimalStructUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		ChunkContextExecutor: func(_ context.Context, state *ChunkIteratorState) error {
			inputVector, err := state.GetInputChunk().GetVector(0)
			if err != nil {
				return err
			}
			input, err := GetStructView(inputVector)
			if err != nil {
				return err
			}

			smallVector, err := input.Field("amounts", "small")
			if err != nil {
				return err
			}
			largeVector, err := input.Field("amounts", "large")
			if err != nil {
				return err
			}
			small, err := GetVectorView[DecimalValue](smallVector)
			if err != nil {
				return err
			}
			large, err := GetVectorView[DecimalValue](largeVector)
			if err != nil {
				return err
			}
			result, err := GetVectorWriter[bool](state.GetResultVector())
			if err != nil {
				return err
			}

			for row := range input.Len() {
				smallValue, smallValid, err := small.Get(row)
				if err != nil {
					return err
				}
				largeValue, largeValid, err := large.Get(row)
				if err != nil {
					return err
				}
				if !smallValid || !largeValid {
					if err = result.SetNull(row); err != nil {
						return err
					}
					continue
				}
				if err = result.Set(row, smallValue.Unscaled == largeValue.Unscaled); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func TestNestedStructViewScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	udf := &nestedDecimalStructUDF{
		inputInfo:  nestedDecimalStructInfo(t),
		resultInfo: mustTypeInfo(t, TYPE_BOOLEAN),
	}
	require.NoError(t, RegisterScalarUDF(conn, "nested_decimal_equal", udf))

	const structType = `STRUCT(amounts STRUCT(small DECIMAL(4,2), large DECIMAL(38,2)))`
	var equal bool
	err := conn.QueryRowContext(ctx, `
		SELECT nested_decimal_equal({
			'amounts': {
				'small': 12.34::DECIMAL(4,2),
				'large': 12.34::DECIMAL(38,2)
			}
		}::`+structType+`)
	`).Scan(&equal)
	require.NoError(t, err)
	require.True(t, equal)

	var nullResult *bool
	err = conn.QueryRowContext(ctx, `
		SELECT nested_decimal_equal({
			'amounts': NULL
		}::`+structType+`)
	`).Scan(&nullResult)
	require.NoError(t, err)
	require.Nil(t, nullResult)
}
