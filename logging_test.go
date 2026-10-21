package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

type logStore struct {
	store []string
}

var myLogStore logStore

func WriteLogEntry(level, logType, logMsg string) {
	myLogStore.store = append(myLogStore.store, level+", "+logType+", "+logMsg)
}

func TestLogStorage(t *testing.T) {
	myLogStore.store = nil
	defer func() {
		err := os.Remove("test_logging.db")
		require.NoError(t, err)
		err = os.Remove("test_logging.db.wal")
		require.NoError(t, err)
	}()

	c := newConnectorWrapper(t, ``, func(execer driver.ExecerContext) error {
		return nil
	})
	defer closeConnectorWrapper(t, c)

	callbacks := LoggerCallbacks{DefaultLoggerCallback: WriteLogEntry}

	err := RegisterLogStorage(c, "MyCustomStorage", callbacks)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	defer closeDbWrapper(t, db)

	// Configure our new log storage.
	_, err = db.Exec("CALL enable_logging(level = 'info');")
	require.NoError(t, err)
	_, err = db.Exec("SET logging_storage = 'MyCustomStorage';")
	require.NoError(t, err)

	// ATTACH a database and silently fail a CHECKPOINT.
	_, err = db.Exec("ATTACH 'test_logging.db'")
	require.NoError(t, err)
	_, err = db.Exec("SET wal_autocheckpoint = '1TB';")
	require.NoError(t, err)
	_, err = db.Exec("SET debug_checkpoint_abort = 'before_header';")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE test_logging.integers AS SELECT * FROM range(100) tbl(i);")
	require.NoError(t, err)

	// Ensure that our log storage contains the logs. DuckDB v2.0.0 also logs
	// each query when duckdb-go prepares it, so collapse those repeats.
	require.Equal(t, []string{
		"INFO, QueryLog, ATTACH 'test_logging.db'",
		"INFO, QueryLog, SET wal_autocheckpoint = '1TB';",
		"INFO, QueryLog, SET debug_checkpoint_abort = 'before_header';",
		"INFO, QueryLog, CREATE TABLE test_logging.integers AS SELECT * FROM range(100) tbl(i);",
	}, slices.Compact(myLogStore.store))
}
