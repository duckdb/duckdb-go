package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestTakeStmtArgs(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: "a"},
		{Ordinal: 2, Value: "b"},
		{Ordinal: 3, Value: "c"},
	}

	taken, rest, err := takeStmtArgs(args, 2)
	require.NoError(t, err)
	require.Len(t, taken, 2)
	require.Equal(t, 1, taken[0].Ordinal)
	require.Equal(t, 2, taken[1].Ordinal)
	require.Equal(t, "a", taken[0].Value)
	require.Equal(t, "b", taken[1].Value)
	require.Len(t, rest, 1)
	require.Equal(t, 3, rest[0].Ordinal)

	_, _, err = takeStmtArgs(rest, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect argument count for command: have 1 want 2")

	empty, rest2, err := takeStmtArgs(rest, 0)
	require.NoError(t, err)
	require.Nil(t, empty)
	require.Equal(t, rest, rest2)

	renumbered := renumberArgs(rest)
	require.Equal(t, 1, renumbered[0].Ordinal)
	require.Equal(t, "c", renumbered[0].Value)
}
