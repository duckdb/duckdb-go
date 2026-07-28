package duckdb

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRowsNextRepeatsEOF pins the driver.Rows contract: once a result is
// exhausted, Next keeps reporting io.EOF.
func TestRowsNextRepeatsEOF(t *testing.T) {
	c := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, c)
	conn := openDriverConnWrapper(t, c)
	defer closeDriverConnWrapper(t, &conn)

	queryer, ok := conn.(driver.QueryerContext)
	require.True(t, ok)

	rows, err := queryer.QueryContext(context.Background(), "SELECT i FROM range(3) AS values(i)", nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	dst := make([]driver.Value, 1)
	for i := range 3 {
		require.NoError(t, rows.Next(dst))
		require.Equal(t, int64(i), dst[0])
	}

	for range 3 {
		require.ErrorIs(t, rows.Next(dst), io.EOF)
	}
}
