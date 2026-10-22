package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// errDBClosedMsg is the text of database/sql's unexported errDBClosed.
const errDBClosedMsg = "sql: database is closed"

func TestConnector_Close(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	// Multiple close calls must not cause panics or errors.
	closeConnectorWrapper(t, c)
	closeConnectorWrapper(t, c)

	_, err := c.Connect(context.Background())
	require.ErrorIs(t, err, errClosedConnector)
}

func TestConnectorConcurrentConnectAndClose(t *testing.T) {
	// sql.DB calls Connector.Connect without holding its lock, so Close can run
	// while new pool connections are being opened.
	for i := range 100 {
		db := sql.OpenDB(newConnectorWrapper(t, ``, nil))

		var wg sync.WaitGroup
		errs := make(chan error, 8)
		stop := make(chan struct{})
		for range cap(errs) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					// Grab a fresh connection to force calls to Connector.Connect.
					conn, err := db.Conn(context.Background())
					if err != nil {
						// A Connect racing DB.Close sees the closed connector. Later
						// calls see the closed sql.DB.
						if !errors.Is(err, errClosedConnector) && err.Error() != errDBClosedMsg {
							errs <- err
							return
						}
						continue
					}
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
					_ = conn.Close()
				}
			}()
		}

		time.Sleep(time.Duration(i%10) * 100 * time.Microsecond)
		closeErr := db.Close()
		close(stop)
		wg.Wait()
		close(errs)
		require.NoError(t, closeErr)
		for err := range errs {
			require.NoError(t, err)
		}

		// Calls that start after DB.Close never reach the connector.
		_, err := db.Conn(context.Background())
		require.EqualError(t, err, errDBClosedMsg)
	}
}
