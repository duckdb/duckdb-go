package duckdb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestCreateRejectedValues(t *testing.T) {
	doubleType := mapping.CreateLogicalType(TYPE_DOUBLE)
	defer mapping.DestroyLogicalType(&doubleType)
	intType := mapping.CreateLogicalType(TYPE_INTEGER)
	defer mapping.DestroyLogicalType(&intType)
	mapType := mapping.CreateMapType(doubleType, intType)
	defer mapping.DestroyLogicalType(&mapType)

	tests := []struct {
		name   string
		create func() (mapping.Value, error)
	}{
		{
			name: "invalid UTF-8 primitive",
			create: func() (mapping.Value, error) {
				return createPrimitiveValue(TYPE_VARCHAR, "\xff")
			},
		},
		{
			name: "duplicate map keys",
			create: func() (mapping.Value, error) {
				return createValue(mapType, OrderedMap{[]any{math.NaN(), math.NaN()}, []any{int32(1), int32(2)}})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.create()
			defer mapping.DestroyValue(&v)
			require.ErrorIs(t, err, errCreateValue)
		})
	}
}

func TestInferRejectedValues(t *testing.T) {
	tests := []struct {
		name string
		arg  any
	}{
		{name: "invalid UTF-8 primitive", arg: "\xff"},
		{name: "invalid UTF-8 list element", arg: []string{"\xff"}},
		{name: "invalid UTF-8 array element", arg: [2]string{"\xff", "ok"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lt, v, err := inferLogicalTypeAndValue(tt.arg)
			defer mapping.DestroyLogicalType(&lt)
			defer mapping.DestroyValue(&v)
			require.ErrorIs(t, err, errCreateValue)
		})
	}
}
