package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

// lockProbeEnv holds the database path that TestConnCloseLockProbeHelper opens in a separate process.
const lockProbeEnv = "DUCKDB_GO_TEST_LOCK_PROBE_PATH"

// TestConnCloseLockProbeHelper is not a real test. It runs in a child process spawned
// by otherProcessOpens and fails if it cannot open the database file.
func TestConnCloseLockProbeHelper(t *testing.T) {
	path := os.Getenv(lockProbeEnv)
	if path == "" {
		return
	}
	db, err := sql.Open(`duckdb`, path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS probe (i INTEGER)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// otherProcessOpens reports whether another process can open the database file,
// i.e., whether this process released the lock on it.
func otherProcessOpens(path string) (bool, string) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestConnCloseLockProbeHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), lockProbeEnv+"="+path)
	out, err := cmd.CombinedOutput()
	return err == nil, string(out)
}

// osThreadCount returns the number of OS threads of this process, or -1 if it cannot be measured.
func osThreadCount() int {
	switch runtime.GOOS {
	case "linux":
		entries, err := os.ReadDir("/proc/self/task")
		if err != nil {
			return -1
		}
		return len(entries)
	case "darwin":
		out, err := exec.Command("ps", "-M", "-p", strconv.Itoa(os.Getpid())).Output()
		if err != nil {
			return -1
		}
		// The first line is the header.
		return len(strings.Split(strings.TrimSpace(string(out)), "\n")) - 1
	}
	return -1
}

// TestConnCloseReleasesDatabase reproduces https://github.com/duckdb/duckdb-go/issues/185.
// A statement prepared on a sql.Conn and never closed must not keep the database
// open (its threads and the file lock) after closing the connection and the database.
func TestConnCloseReleasesDatabase(t *testing.T) {
	const threads = 8

	tests := []struct {
		name      string
		closeStmt bool
	}{
		{name: "control: statement closed before the connection", closeStmt: true},
		{name: "statement left open", closeStmt: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "conn_close.db")

			db := openDbWrapper(t, fmt.Sprintf("%s?threads=%d", path, threads))
			conn := openConnWrapper(t, db, ctx)
			stmt, err := conn.PrepareContext(ctx, `SELECT 42`)
			require.NoError(t, err)

			var v int
			require.NoError(t, stmt.QueryRowContext(ctx).Scan(&v))
			require.Equal(t, 42, v)
			during := osThreadCount()

			if tt.closeStmt {
				require.NoError(t, stmt.Close())
			}
			require.NoError(t, conn.Close())
			require.NoError(t, db.Close())
			// Measure before spawning the child process, which can start more runtime threads.
			after := osThreadCount()

			opened, out := otherProcessOpens(path)
			require.True(t, opened, "another process cannot open the database file after DB.Close:\n%s", out)

			// The worker threads of the database instance are gone. A surviving instance keeps
			// all of them, so a margin of half the pool tolerates unrelated runtime threads.
			if during >= 0 && after >= 0 {
				require.LessOrEqual(t, after, during-threads/2,
					"threads during=%d after DB.Close=%d", during, after)
			}

			// Closing the statement after the database is a no-op.
			if !tt.closeStmt {
				require.NoError(t, stmt.Close())
			}
		})
	}
}

// TestConnCloseInvalidatesStmts checks the driver.Conn contract:
// Close invalidates the prepared statements of the connection.
func TestConnCloseInvalidatesStmts(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	defer closeConnectorWrapper(t, c)

	tests := []struct {
		name    string
		use     func(s *Stmt) error
		wantErr error
	}{
		{
			name: "ExecContext",
			use: func(s *Stmt) error {
				_, err := s.ExecContext(context.Background(), nil)
				return err
			},
			wantErr: errClosedStmt,
		},
		{
			name: "QueryContext",
			use: func(s *Stmt) error {
				_, err := s.QueryContext(context.Background(), nil)
				return err
			},
			wantErr: errClosedStmt,
		},
		{
			name: "ExecBound",
			use: func(s *Stmt) error {
				_, err := s.ExecBound(context.Background())
				return err
			},
			wantErr: errClosedCon,
		},
		{
			name: "QueryBound",
			use: func(s *Stmt) error {
				_, err := s.QueryBound(context.Background())
				return err
			},
			wantErr: errClosedCon,
		},
		{
			name:    "Bind",
			use:     func(s *Stmt) error { return s.Bind(nil) },
			wantErr: errClosedStmt,
		},
		{
			name: "ColumnCount",
			use: func(s *Stmt) error {
				_, err := s.ColumnCount()
				return err
			},
			wantErr: errClosedStmt,
		},
		{
			name: "StatementType",
			use: func(s *Stmt) error {
				_, err := s.StatementType()
				return err
			},
			wantErr: errClosedStmt,
		},
		{
			name: "Close",
			use:  func(s *Stmt) error { return s.Close() },
		},
		{
			name: "Close twice",
			use: func(s *Stmt) error {
				return errors.Join(s.Close(), s.Close())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driverConn := openDriverConnWrapper(t, c)
			conn := driverConn.(*Conn)
			s, err := conn.PrepareContext(context.Background(), `SELECT 42`)
			require.NoError(t, err)
			stmt := s.(*Stmt)

			require.NoError(t, conn.Close())
			require.True(t, stmt.closed)
			require.Empty(t, conn.stmts)
			require.Equal(t, -1, stmt.NumInput())

			err = tt.use(stmt)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestConnCloseStmtBookkeeping checks that the connection only keeps the statements that are still open.
func TestConnCloseStmtBookkeeping(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	defer closeConnectorWrapper(t, c)

	driverConn := openDriverConnWrapper(t, c)
	conn := driverConn.(*Conn)

	closed, err := conn.PrepareContext(context.Background(), `SELECT 1`)
	require.NoError(t, err)
	open, err := conn.PrepareContext(context.Background(), `SELECT 2`)
	require.NoError(t, err)
	require.Len(t, conn.stmts, 2)

	// Statements used internally by ExecContext and QueryContext do not stay behind.
	_, err = conn.ExecContext(context.Background(), `SELECT 3; SELECT 4`, nil)
	require.NoError(t, err)
	rows, err := conn.QueryContext(context.Background(), `SELECT 5`, nil)
	require.NoError(t, err)
	require.Len(t, conn.stmts, 3)
	require.NoError(t, rows.Close())
	require.Len(t, conn.stmts, 2)

	// A statement closed by the caller is not destroyed again by Conn.Close.
	require.NoError(t, closed.Close())
	require.Len(t, conn.stmts, 1)
	require.Contains(t, conn.stmts, open.(*Stmt))
	require.Panics(t, func() { _ = closed.Close() })

	require.NoError(t, conn.Close())
	require.False(t, closed.(*Stmt).closedByConn)
	require.True(t, open.(*Stmt).closedByConn)
}

// TestConnCloseWithActiveRows closes a connection while the rows of one of its statements are still open.
func TestConnCloseWithActiveRows(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	defer closeConnectorWrapper(t, c)

	driverConn := openDriverConnWrapper(t, c)
	conn := driverConn.(*Conn)

	rows, err := conn.QueryContext(context.Background(), `SELECT range AS i FROM range(3)`, nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// The result is still readable, and closing the rows (and with them the statement) is safe.
	dest := make([]driver.Value, 1)
	require.NoError(t, rows.Next(dest))
	require.Equal(t, int64(0), dest[0])
	require.NoError(t, rows.Close())
}

// TestConnCloseConcurrentStmtClose closes statements and their connection concurrently.
// Each statement must be destroyed exactly once. Run with -race.
func TestConnCloseConcurrentStmtClose(t *testing.T) {
	const (
		rounds   = 20
		numStmts = 16
	)

	c := newConnectorWrapper(t, ``, nil)
	defer closeConnectorWrapper(t, c)

	for range rounds {
		driverConn := openDriverConnWrapper(t, c)
		conn := driverConn.(*Conn)

		stmts := make([]driver.Stmt, numStmts)
		for i := range stmts {
			s, err := conn.PrepareContext(context.Background(), `SELECT 42`)
			require.NoError(t, err)
			stmts[i] = s
		}

		var wg sync.WaitGroup
		errs := make([]error, numStmts)
		for i, s := range stmts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = s.Close()
			}()
		}
		require.NoError(t, conn.Close())
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}
		require.Empty(t, conn.stmts)
	}
}
