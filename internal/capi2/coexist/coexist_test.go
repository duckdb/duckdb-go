// Coexistence has its own test binary so driver tests stay v1-only and
// standalone capi2 tests stay v1-free.
package coexist_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/duckdb/duckdb-go/v2/internal/capi2"
)

func TestCAPI2CoexistsWithV1(t *testing.T) {
	// V1's package initializer has already registered the same static extension
	// describe functions. A second identical registration must remain harmless.
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var value int
	require.NoError(t, db.QueryRow("SELECT 19").Scan(&value))
	require.Equal(t, 19, value)
	require.NoError(t, capi2.Bootstrap())
	require.NoError(t, capi2.Bootstrap())
	env, err := capi2.NewEnvironment()
	require.NoError(t, err)
	defer func() { require.NoError(t, env.Close()) }()
	instance, err := env.NewInstance()
	require.NoError(t, err)
	defer func() { require.NoError(t, instance.Close()) }()
	require.NoError(t, instance.Attach(":memory:"))
	conn, err := instance.Connect()
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	statements, err := conn.Parse("SELECT json_valid('{}')::BIGINT + 72")
	require.NoError(t, err)
	defer func() { require.NoError(t, statements.Close()) }()
	statement, err := statements.Next()
	require.NoError(t, err)
	require.NotNil(t, statement)
	defer func() { require.NoError(t, statement.Close()) }()
	result, err := conn.Execute(statement)
	require.NoError(t, err)
	defer func() { require.NoError(t, result.Close()) }()
	chunk, err := result.Fetch()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	defer func() { require.NoError(t, chunk.Close()) }()
	native, err := chunk.Int64(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(73), native)
	_, err = result.Drain()
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT 31").Scan(&value))
	require.Equal(t, 31, value)
}
