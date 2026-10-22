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

	"github.com/duckdb/duckdb-go/v2/mapping"
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

// This test patches package-level mapping hooks and must not run in parallel.
func TestConnectorCloseWaitsForConnect(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)

	// Hold Connect inside the native call. The stub never touches the database,
	// so a broken Close cannot turn this test into a use-after-free.
	entered := make(chan struct{})
	release := make(chan struct{})
	patchVar(t, &mapping.Connect, func(mapping.Database, *mapping.Connection) mapping.State {
		close(entered)
		<-release
		return mapping.StateError
	})

	connErr := make(chan error, 1)
	go func() {
		_, err := c.Connect(context.Background())
		connErr <- err
	}()
	<-entered

	closed, waitClose := closeWithin(c, 100*time.Millisecond)
	close(release)
	err := <-connErr
	closeErr := waitClose()
	require.False(t, closed, "Close returned while Connect was using the database")
	require.NoError(t, closeErr)
	require.ErrorIs(t, err, errConnect)
}

func TestConnectorCloseDuringConnInit(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := newConnectorWrapper(t, ``, func(driver.ExecerContext) error {
		close(entered)
		<-release
		return nil
	})

	type result struct {
		conn driver.Conn
		err  error
	}
	res := make(chan result, 1)
	go func() {
		conn, err := c.Connect(context.Background())
		res <- result{conn, err}
	}()
	<-entered

	// Close must not wait for the initializer, which runs user code.
	closed, waitClose := closeWithin(c, 10*time.Second)
	close(release)
	got := <-res
	if got.conn != nil {
		defer closeDriverConnWrapper(t, &got.conn)
	}
	closeErr := waitClose()
	require.True(t, closed, "Close blocked on the connection initializer")
	require.NoError(t, closeErr)
	require.NoError(t, got.err)

	// The connection outlives the closed connector.
	queryer := got.conn.(driver.QueryerContext)
	rows, err := queryer.QueryContext(context.Background(), `SELECT 42`, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.Equal(t, int32(42), values[0])
}

// closeWithin starts c.Close and reports whether it returned within d. Call the
// returned wait function before asserting, so that Close never outlives the test.
func closeWithin(c *Connector, d time.Duration) (bool, func() error) {
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		return true, func() error { return err }
	case <-time.After(d):
		return false, func() error { return <-done }
	}
}
