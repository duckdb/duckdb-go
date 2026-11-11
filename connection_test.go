package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactions(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)
	_, err := conn.ExecContext(ctx, `CREATE TABLE tx_test (value INTEGER)`)
	require.NoError(t, err)

	for _, commit := range []bool{true, false} {
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO tx_test VALUES (42)`)
		require.NoError(t, err)
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM tx_test`).Scan(&count))
		if commit {
			require.Equal(t, 1, count)
			require.NoError(t, tx.Commit())
		} else {
			require.Equal(t, 2, count)
			require.NoError(t, tx.Rollback())
		}
		// Both completion paths must leave the same connection usable.
		require.NoError(t, conn.QueryRowContext(ctx, `SELECT count(*) FROM tx_test`).Scan(&count))
		require.Equal(t, 1, count)
	}

	// A second connection observes the committed row, but not the rolled-back row.
	var values []int
	rows, err := db.Query(`SELECT value FROM tx_test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{42}, values)
}

func TestTransactionOptions(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)
	for _, tt := range []struct {
		name string
		opts sql.TxOptions
		err  string
	}{
		{"read only", sql.TxOptions{ReadOnly: true}, "read-only transactions are not supported"},
		{"isolation", sql.TxOptions{Isolation: sql.LevelSerializable}, "isolation level not supported"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := conn.BeginTx(ctx, &tt.opts)
			require.ErrorContains(t, err, tt.err)
			require.Nil(t, tx)
			// Rejected options must not leave an active transaction behind.
			tx, err = conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestMultiStatementParameters(t *testing.T) {
	intermediateError := errParametersBeforeLastStmt.Error()
	for _, method := range []string{"exec", "query", "prepare"} {
		for _, tt := range []struct {
			name  string
			query string
			args  []any
			err   string
		}{
			{"first", `SET VARIABLE value = ?; SET VARIABLE later = 'reached';`, []any{"duck"}, intermediateError},
			{"middle", `SET VARIABLE earlier = 'camel'; SET VARIABLE value = ?; SET VARIABLE later = 'reached';`, []any{"duck"}, intermediateError},
			{"named", `SET VARIABLE value = $bird; SET VARIABLE later = 'reached';`, []any{sql.Named("bird", "duck")}, intermediateError},
			{"last", `SET VARIABLE earlier = 'camel'; SET VARIABLE value = ?;`, []any{"duck"}, ""},
			{"missing final argument", `SET VARIABLE earlier = 'camel'; SET VARIABLE value = ?;`, nil, "incorrect argument count for command: have 0 want 1"},
		} {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				db := openDbWrapper(t, ``)
				defer closeDbWrapper(t, db)
				ctx := context.Background()
				conn := openConnWrapper(t, db, ctx)
				defer closeConnWrapper(t, conn)

				var err error
				switch method {
				case "exec":
					_, err = conn.ExecContext(ctx, tt.query, tt.args...)
				case "query":
					var rows *sql.Rows
					rows, err = conn.QueryContext(ctx, tt.query, tt.args...)
					if rows != nil {
						closeRowsWrapper(t, rows)
					}
				case "prepare":
					var stmt *sql.Stmt
					stmt, err = conn.PrepareContext(ctx, tt.query)
					if err == nil {
						defer closePreparedWrapper(t, stmt)
						_, err = stmt.ExecContext(ctx, tt.args...)
						// database/sql checks prepared statement arity before the driver.
						if tt.name == "missing final argument" {
							require.EqualError(t, err, "sql: expected 1 arguments, got 0")
							return
						}
					}
				}
				if tt.err != "" {
					require.EqualError(t, err, tt.err)
					var later sql.NullString
					require.NoError(t, conn.QueryRowContext(ctx, `SELECT getvariable('later')`).Scan(&later))
					require.False(t, later.Valid, "statements after the error must not execute")
					return
				}
				require.NoError(t, err)
				var value string
				require.NoError(t, conn.QueryRowContext(ctx, `SELECT getvariable('value')`).Scan(&value))
				require.Equal(t, "duck", value)
			})
		}
	}
}

func TestGetTableNames(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	tests := []struct {
		name           string
		query          string
		qualified      bool
		expectedTables []string
		expectedError  string
	}{
		{
			name:           "valid query with multiple tables, qualified",
			query:          `SELECT * FROM schema1.table1, catalog3."schema.2"."table.2"`,
			qualified:      true,
			expectedTables: []string{"schema1.table1", `catalog3."schema.2"."table.2"`},
		},
		{
			name:           "valid query with multiple tables, unqualified",
			query:          `SELECT * FROM schema1.table1, catalog3."schema.2"."table.2"`,
			qualified:      false,
			expectedTables: []string{"table1", "table.2"},
		},
		{
			name:           "valid query with no tables",
			query:          "SELECT 1 as num",
			expectedTables: nil,
		},
		{
			name:          "invalid query syntax",
			query:         "SELECT * FROM WHERE",
			expectedError: "Parser Error: syntax error at or near \"WHERE\"",
		},
		{
			name:          "empty query",
			query:         "",
			expectedError: "empty query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableNames, err := GetTableNames(conn, tt.query, tt.qualified)
			if tt.expectedError != "" {
				require.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, tableNames)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tt.expectedTables, tableNames)
			}
		})
	}
}
