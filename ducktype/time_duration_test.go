package ducktype_test

import (
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2/ducktype"
	"github.com/stretchr/testify/require"
)

func TestNullDuration(t *testing.T) {
	t.Run("Scan", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			var n ducktype.NullDuration
			require.NoError(t, n.Scan(nil))
			require.False(t, n.Valid)
			require.Equal(t, time.Duration(0), n.V)
		})

		t.Run("int64", func(t *testing.T) {
			var n ducktype.NullDuration
			require.NoError(t, n.Scan(int64(time.Second)))
			require.True(t, n.Valid)
			require.Equal(t, time.Second, n.V)
		})

		t.Run("float64", func(t *testing.T) {
			var n ducktype.NullDuration
			require.NoError(t, n.Scan(float64(time.Millisecond)))
			require.True(t, n.Valid)
			require.Equal(t, time.Millisecond, n.V)
		})

		t.Run("bytes", func(t *testing.T) {
			var n ducktype.NullDuration
			require.NoError(t, n.Scan([]byte("1000000000"))) // 1 second in nanoseconds
			require.True(t, n.Valid)
			require.Equal(t, time.Second, n.V)
		})

		t.Run("bytes invalid", func(t *testing.T) {
			var n ducktype.NullDuration
			require.Error(t, n.Scan([]byte("notanumber")))
		})

		t.Run("string", func(t *testing.T) {
			var n ducktype.NullDuration
			require.NoError(t, n.Scan("1h30m"))
			require.True(t, n.Valid)
			require.Equal(t, 90*time.Minute, n.V)
		})

		t.Run("string invalid", func(t *testing.T) {
			var n ducktype.NullDuration
			require.Error(t, n.Scan("notaduration"))
		})

		t.Run("interval via JSON days and micros", func(t *testing.T) {
			type Interval struct {
				Days   int32 `json:"days"`
				Months int32 `json:"months"`
				Micros int64 `json:"micros"`
			}
			var n ducktype.NullDuration
			require.NoError(t, n.Scan(Interval{Days: 1, Months: 0, Micros: 500000}))
			require.True(t, n.Valid)
			require.Equal(t, 24*time.Hour+500*time.Millisecond, n.V)
		})

		t.Run("interval via JSON months", func(t *testing.T) {
			type Interval struct {
				Days   int32 `json:"days"`
				Months int32 `json:"months"`
				Micros int64 `json:"micros"`
			}
			var n ducktype.NullDuration
			require.NoError(t, n.Scan(Interval{Days: 0, Months: 1, Micros: 0}))
			require.True(t, n.Valid)
			require.Equal(t, 30*24*time.Hour, n.V)
		})

		t.Run("unsupported type", func(t *testing.T) {
			var n ducktype.NullDuration
			require.Error(t, n.Scan(func() {}))
		})
	})

	t.Run("Value", func(t *testing.T) {
		t.Run("invalid", func(t *testing.T) {
			n := ducktype.NullDuration{Valid: false}
			v, err := n.Value()
			require.NoError(t, err)
			require.Nil(t, v)
		})

		t.Run("valid", func(t *testing.T) {
			n := ducktype.NullDuration{V: 90 * time.Minute, Valid: true}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, "1h30m0s", v)
		})
	})

	t.Run("select from duckdb", func(t *testing.T) {
		t.Run("interval 1 second", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullDuration
			row := db.QueryRowContext(t.Context(), "SELECT INTERVAL '1' SECOND")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, time.Second, n.V)
		})

		t.Run("null", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullDuration
			row := db.QueryRowContext(t.Context(), "SELECT NULL::INTERVAL")
			require.NoError(t, row.Scan(&n))
			require.False(t, n.Valid)
		})
	})

	t.Run("insert into duckdb", func(t *testing.T) {
		t.Run("interval column", func(t *testing.T) {
			db := newDB(t)
			_, err := db.ExecContext(t.Context(), `CREATE TABLE t (d INTERVAL NOT NULL)`)
			require.NoError(t, err)

			in := ducktype.NullDuration{V: 90 * time.Minute, Valid: true}
			_, err = db.ExecContext(t.Context(), `INSERT INTO t (d) VALUES ($1)`, in)
			require.NoError(t, err)

			var out ducktype.NullDuration
			row := db.QueryRowContext(t.Context(), `SELECT d FROM t`)
			require.NoError(t, row.Scan(&out))
			require.True(t, out.Valid)
			require.Equal(t, in.V, out.V)
		})
	})
}
