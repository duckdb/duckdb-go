//go:build debug_bindings

package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// preparedStmtCount returns the number of native prepared statements that are still allocated.
// The counters only exist with the debug_bindings build tag.
func preparedStmtCount() int {
	count, _ := mapping.GetAllocationCount(mapping.AllocationCounterPreparedStatement)
	return count
}

// TestConnCloseAllocationBalance checks that closing a connection releases the native handles
// of its prepared statements, and that each statement is released exactly once.
func TestConnCloseAllocationBalance(t *testing.T) {
	tests := []struct {
		name           string
		prepared       int
		closedByCaller int
	}{
		{name: "no statements", prepared: 0, closedByCaller: 0},
		{name: "one statement left open", prepared: 1, closedByCaller: 0},
		{name: "several statements left open", prepared: 8, closedByCaller: 0},
		{name: "some statements closed by the caller", prepared: 8, closedByCaller: 3},
		{name: "control: all statements closed by the caller", prepared: 8, closedByCaller: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := newConnectorWrapper(t, ``, nil)
			defer closeConnectorWrapper(t, c)
			conn := openDriverConnWrapper(t, c).(*Conn)
			before := preparedStmtCount()

			stmts := make([]driver.Stmt, tt.prepared)
			for i := range stmts {
				s, err := conn.PrepareContext(ctx, `SELECT 42`)
				require.NoError(t, err)
				stmts[i] = s
			}
			require.Equal(t, before+tt.prepared, preparedStmtCount())

			for _, s := range stmts[:tt.closedByCaller] {
				require.NoError(t, s.Close())
			}
			require.Equal(t, before+tt.prepared-tt.closedByCaller, preparedStmtCount())

			require.NoError(t, conn.Close())
			require.Equal(t, before, preparedStmtCount())

			// Closing a statement that Conn.Close destroyed does not release it again.
			for _, s := range stmts[tt.closedByCaller:] {
				require.NoError(t, s.Close())
			}
			require.Equal(t, before, preparedStmtCount())
		})
	}

	// The scenario of https://github.com/duckdb/duckdb-go/issues/185:
	// database/sql does not track statements prepared on a sql.Conn.
	t.Run("statement left open on a sql.Conn", func(t *testing.T) {
		ctx := context.Background()
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		conn := openConnWrapper(t, db, ctx)
		before := preparedStmtCount()

		_, err := conn.PrepareContext(ctx, `SELECT 42`)
		require.NoError(t, err)
		require.Equal(t, before+1, preparedStmtCount())

		require.NoError(t, conn.Close())
		require.NoError(t, db.Close())
		require.Equal(t, before, preparedStmtCount())
	})

	t.Run("connection closed with active rows", func(t *testing.T) {
		ctx := context.Background()
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)
		conn := openDriverConnWrapper(t, c).(*Conn)
		before := preparedStmtCount()

		rows, err := conn.QueryContext(ctx, `SELECT range AS i FROM range(3)`, nil)
		require.NoError(t, err)
		require.Equal(t, before+1, preparedStmtCount())

		require.NoError(t, conn.Close())
		require.Equal(t, before, preparedStmtCount())
		require.NoError(t, rows.Close())
		require.Equal(t, before, preparedStmtCount())
	})
}
