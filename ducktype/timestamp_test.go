package ducktype_test

import (
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2/ducktype"
	"github.com/stretchr/testify/require"
)

func TestInfinityModifier(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		t.Run("none", func(t *testing.T) {
			require.Equal(t, "none", ducktype.None.String())
		})
		t.Run("infinity", func(t *testing.T) {
			require.Equal(t, "infinity", ducktype.Infinity.String())
		})
		t.Run("negative infinity", func(t *testing.T) {
			require.Equal(t, "-infinity", ducktype.NegativeInfinity.String())
		})
		t.Run("invalid", func(t *testing.T) {
			require.Equal(t, "invalid", ducktype.InfinityModifier(99).String())
		})
	})
}

func TestNullTime(t *testing.T) {
	t.Run("Infinity", func(t *testing.T) {
		var n ducktype.NullTime
		n.Infinity()
		require.Equal(t, ducktype.Present, n.Status)
		require.Equal(t, ducktype.Infinity, n.InfinityModifier)
	})

	t.Run("NegativeInfinity", func(t *testing.T) {
		var n ducktype.NullTime
		n.NegativeInfinity()
		require.Equal(t, ducktype.Present, n.Status)
		require.Equal(t, ducktype.NegativeInfinity, n.InfinityModifier)
	})

	t.Run("Scan", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			var n ducktype.NullTime
			require.NoError(t, n.Scan(nil))
			require.Equal(t, ducktype.Null, n.Status)
		})

		t.Run("time", func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			var n ducktype.NullTime
			require.NoError(t, n.Scan(now))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.None, n.InfinityModifier)
			require.Equal(t, now, n.Time)
		})

		t.Run("infinity sentinel", func(t *testing.T) {
			var n ducktype.NullTime
			require.NoError(t, n.Scan(time.UnixMicro(9223372036854775807)))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.Infinity, n.InfinityModifier)
		})

		t.Run("negative infinity sentinel", func(t *testing.T) {
			var n ducktype.NullTime
			require.NoError(t, n.Scan(time.UnixMicro(-9223372036854775807)))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.NegativeInfinity, n.InfinityModifier)
		})

		t.Run("unsupported type", func(t *testing.T) {
			var n ducktype.NullTime
			require.Error(t, n.Scan("not-a-time"))
		})
	})

	t.Run("Value", func(t *testing.T) {
		t.Run("null", func(t *testing.T) {
			n := ducktype.NullTime{Status: ducktype.Null}
			v, err := n.Value()
			require.NoError(t, err)
			require.Nil(t, v)
		})

		t.Run("undefined", func(t *testing.T) {
			n := ducktype.NullTime{Status: ducktype.Undefined}
			_, err := n.Value()
			require.Error(t, err)
		})

		t.Run("present", func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			n := ducktype.NullTime{Time: now, Status: ducktype.Present}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, now, v)
		})

		t.Run("infinity", func(t *testing.T) {
			n := ducktype.NullTime{Status: ducktype.Present, InfinityModifier: ducktype.Infinity}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, "infinity", v)
		})

		t.Run("negative infinity", func(t *testing.T) {
			n := ducktype.NullTime{Status: ducktype.Present, InfinityModifier: ducktype.NegativeInfinity}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, "-infinity", v)
		})
	})

	t.Run("select from duckdb", func(t *testing.T) {
		t.Run("now", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullTime
			row := db.QueryRowContext(t.Context(), "SELECT now()::TIMESTAMPTZ")
			require.NoError(t, row.Scan(&n))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.None, n.InfinityModifier)
			require.False(t, n.Time.IsZero())
		})

		t.Run("null", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullTime
			row := db.QueryRowContext(t.Context(), "SELECT NULL::TIMESTAMPTZ")
			require.NoError(t, row.Scan(&n))
			require.Equal(t, ducktype.Null, n.Status)
		})

		t.Run("infinity", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullTime
			row := db.QueryRowContext(t.Context(), "SELECT 'infinity'::TIMESTAMPTZ")
			require.NoError(t, row.Scan(&n))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.Infinity, n.InfinityModifier)
		})

		t.Run("negative infinity", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullTime
			row := db.QueryRowContext(t.Context(), "SELECT '-infinity'::TIMESTAMPTZ")
			require.NoError(t, row.Scan(&n))
			require.Equal(t, ducktype.Present, n.Status)
			require.Equal(t, ducktype.NegativeInfinity, n.InfinityModifier)
		})

		t.Run("roundtrip", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE ts_test (ts TIMESTAMPTZ)")
			require.NoError(t, err)

			now := time.Now().UTC().Truncate(time.Microsecond)
			in := ducktype.NullTime{Time: now, Status: ducktype.Present}
			_, err = db.ExecContext(ctx, "INSERT INTO ts_test VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullTime
			row := db.QueryRowContext(ctx, "SELECT ts FROM ts_test")
			require.NoError(t, row.Scan(&out))
			require.Equal(t, ducktype.Present, out.Status)
			require.True(t, now.Equal(out.Time))
		})
	})
}
