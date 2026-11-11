package duckdb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestUUIDValueBindRoundTrip(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	_, err := db.Exec(`CREATE TABLE t (ids UUID[], arr UUID[1], s STRUCT(a UUID), m MAP(UUID, UUID))`)
	require.NoError(t, err)
	for _, want := range []string{
		"123e4567-e89b-12d3-a456-426614174000",
		"923e4567-e89b-12d3-a456-426614174000",
	} {
		t.Run(want, func(t *testing.T) {
			var u UUID
			require.NoError(t, u.Scan(want))
			_, err := db.Exec(`DELETE FROM t`)
			require.NoError(t, err)
			_, err = db.Exec(`INSERT INTO t VALUES (?, ?, ?, ?)`,
				[]UUID{u}, [1]UUID{u}, map[string]any{"a": u}, Map{u: u})
			require.NoError(t, err)
			var list, array, st, mapKey, mapValue string
			require.NoError(t, db.QueryRow(`SELECT ids[1]::VARCHAR, arr[1]::VARCHAR, s.a::VARCHAR,
				map_keys(m)[1]::VARCHAR, map_values(m)[1]::VARCHAR FROM t`).
				Scan(&list, &array, &st, &mapKey, &mapValue))
			for name, got := range map[string]string{
				"list": list, "array": array, "struct": st, "map key": mapKey, "map value": mapValue,
			} {
				t.Run(name, func(t *testing.T) { require.Equal(t, want, got) })
			}
			t.Run("untyped", func(t *testing.T) {
				var got string
				require.NoError(t, db.QueryRow(`SELECT x::VARCHAR FROM (SELECT ? AS x)`, u).Scan(&got))
				require.Equal(t, want, got)
			})
		})
	}
}

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
