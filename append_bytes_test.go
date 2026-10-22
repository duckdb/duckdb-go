package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendBytes_varcharRoundTrip(t *testing.T) {
	c := newConnectorWrapper(t, ":memory:", nil)
	defer closeConnectorWrapper(t, c)

	conn := openDriverConnWrapper(t, c)
	defer closeDriverConnWrapper(t, &conn)

	_, err := conn.ExecContext(context.Background(), `CREATE TABLE t (payload VARCHAR)`, nil)
	require.NoError(t, err)

	a := newAppenderWrapper(t, &conn, "", "t")
	defer closeAppenderWrapper(t, a)

	payload := []byte(`{"k":"v"}`)
	require.NoError(t, a.AppendRow(AppendBytesUnsafe(payload)))
	require.NoError(t, a.Close())

	rows, err := conn.QueryContext(context.Background(), `SELECT payload FROM t`, nil)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)

	require.True(t, rows.Next())
	var got string
	require.NoError(t, rows.Scan(&got))
	require.Equal(t, string(payload), got)
}
