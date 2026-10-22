package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// DuckDB's appender flushes automatically every 100 data chunks
// (BaseAppender::DEFAULT_FLUSH_COUNT).
var automaticFlushRows = 100 * GetDataChunkCapacity()

const primaryKeyTableSQL = `CREATE TABLE test (i INTEGER PRIMARY KEY)`

func prepareAutomaticFlushFailure(
	t *testing.T,
	appenderType string,
	rowCount int,
) (*sql.DB, *Appender, *Error) {
	t.Helper()
	c, db, conn, a := prepareAppender(t, appenderType, primaryKeyTableSQL)
	t.Cleanup(func() { cleanupDb(t, c, db, conn) })

	require.NoError(t, a.AppendRow(int32(-1)))
	require.NoError(t, a.AppendRow(int32(-1)))
	var appendErr error
	for i := range rowCount {
		if err := a.AppendRow(int32(i)); err != nil {
			require.ErrorContains(t, err, "duplicate key")
			appendErr = err
		}
	}
	var duckErr *Error
	require.ErrorAs(t, appendErr, &duckErr, "the duplicate must trigger an automatic flush failure")
	return db, a, duckErr
}

func countRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM test`).Scan(&count))
	return count
}

func TestAppenderCloseAfterAutomaticFlushFailure(t *testing.T) {
	for _, appenderType := range appenderTypes {
		t.Run(appenderType, func(t *testing.T) {
			t.Run("close", func(t *testing.T) {
				// The first automatic flush fails on the duplicate, and Close flushes the rest.
				rowCount := automaticFlushRows + automaticFlushRows/2
				db, a, duckErr := prepareAutomaticFlushFailure(t, appenderType, rowCount)
				err := a.Close()
				require.ErrorIs(t, err, errAppenderClose)
				require.ErrorIs(t, err, duckErr)

				// Later batches still commit even though Close reports the lost batch. The failed
				// flush drops its whole batch and the row whose append triggered it.
				require.Equal(t, rowCount+2-automaticFlushRows-1, countRows(t, db))
			})

			t.Run("clear then close", func(t *testing.T) {
				// The first automatic flush fails on the duplicate and the second commits,
				// leaving rows buffered for Clear to discard.
				rowCount := 2*automaticFlushRows + automaticFlushRows/2
				db, a, _ := prepareAutomaticFlushFailure(t, appenderType, rowCount)
				committedBeforeClear := countRows(t, db)
				require.Positive(t, committedBeforeClear)
				require.NoError(t, a.Clear())
				require.NoError(t, a.AppendRow(int32(-2)))
				require.NoError(t, a.Close())
				require.Equal(t, committedBeforeClear+1, countRows(t, db))
			})
		})
	}
}

func TestAppenderCloseAfterExplicitFlushFailure(t *testing.T) {
	for _, appenderType := range appenderTypes {
		t.Run(appenderType, func(t *testing.T) {
			testFlush := func(t *testing.T, flush func(*Appender) error) {
				t.Helper()
				c, db, conn, a := prepareAppender(t, appenderType, primaryKeyTableSQL)
				defer cleanupDb(t, c, db, conn)

				require.NoError(t, a.AppendRow(int32(42)))
				require.NoError(t, a.AppendRow(int32(42)))
				flushErr := flush(a)
				require.ErrorContains(t, flushErr, "duplicate key")

				// A later successful flush must not acknowledge the lost batch.
				require.NoError(t, a.AppendRow(int32(7)))
				require.NoError(t, flush(a))
				var duckErr *Error
				require.ErrorAs(t, flushErr, &duckErr)
				require.ErrorIs(t, a.Close(), duckErr)
			}

			t.Run("Flush", func(t *testing.T) {
				testFlush(t, (*Appender).Flush)
			})

			t.Run("FlushWithCancel", func(t *testing.T) {
				testFlush(t, func(a *Appender) error {
					return a.FlushWithCancel(context.Background())
				})
			})
		})
	}
}
