package ducktype_test

import (
	"testing"

	"github.com/duckdb/duckdb-go/v2/ducktype"
	"github.com/stretchr/testify/require"
)

func TestNullUint64(t *testing.T) {
	t.Run("Scan", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan(nil))
			require.False(t, n.Valid)
			require.Equal(t, uint64(0), n.V)
		})

		t.Run("uint64", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan(uint64(42)))
			require.True(t, n.Valid)
			require.Equal(t, uint64(42), n.V)
		})

		t.Run("uint64 max value", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan(uint64(^uint64(0))))
			require.True(t, n.Valid)
			require.Equal(t, uint64(^uint64(0)), n.V)
		})

		t.Run("int64", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan(int64(100)))
			require.True(t, n.Valid)
			require.Equal(t, uint64(100), n.V)
		})

		t.Run("int64 negative", func(t *testing.T) {
			var n ducktype.NullUint64
			require.Error(t, n.Scan(int64(-1)))
		})

		t.Run("float64", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan(float64(99.9)))
			require.True(t, n.Valid)
			require.Equal(t, uint64(99), n.V)
		})

		t.Run("float64 negative", func(t *testing.T) {
			var n ducktype.NullUint64
			require.Error(t, n.Scan(float64(-1.0)))
		})

		t.Run("bytes", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan([]byte("12345")))
			require.True(t, n.Valid)
			require.Equal(t, uint64(12345), n.V)
		})

		t.Run("bytes invalid", func(t *testing.T) {
			var n ducktype.NullUint64
			require.Error(t, n.Scan([]byte("notanumber")))
		})

		t.Run("string", func(t *testing.T) {
			var n ducktype.NullUint64
			require.NoError(t, n.Scan("99999"))
			require.True(t, n.Valid)
			require.Equal(t, uint64(99999), n.V)
		})

		t.Run("string invalid", func(t *testing.T) {
			var n ducktype.NullUint64
			require.Error(t, n.Scan("notanumber"))
		})

		t.Run("unsupported type", func(t *testing.T) {
			var n ducktype.NullUint64
			require.Error(t, n.Scan(true))
		})
	})

	t.Run("Value", func(t *testing.T) {
		t.Run("invalid", func(t *testing.T) {
			n := ducktype.NullUint64{Valid: false}
			v, err := n.Value()
			require.NoError(t, err)
			require.Nil(t, v)
		})

		t.Run("valid", func(t *testing.T) {
			n := ducktype.NullUint64{V: 1234567890, Valid: true}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, "1234567890", v)
		})
	})

	t.Run("select from duckdb", func(t *testing.T) {
		t.Run("ubigint", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullUint64
			row := db.QueryRowContext(t.Context(), "SELECT 42::UBIGINT")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, uint64(42), n.V)
		})

		t.Run("null", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullUint64
			row := db.QueryRowContext(t.Context(), "SELECT NULL::UBIGINT")
			require.NoError(t, row.Scan(&n))
			require.False(t, n.Valid)
		})

		t.Run("roundtrip max value", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE u64_test (v UBIGINT)")
			require.NoError(t, err)

			in := ducktype.NullUint64{V: 18446744073709551615, Valid: true}
			_, err = db.ExecContext(ctx, "INSERT INTO u64_test VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullUint64
			row := db.QueryRowContext(ctx, "SELECT v FROM u64_test")
			require.NoError(t, row.Scan(&out))
			require.True(t, out.Valid)
			require.Equal(t, uint64(18446744073709551615), out.V)
		})
	})
}
