package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestStreamingResultsReadsEveryRow(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()), `SELECT i FROM range(1000000) t(i)`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	var n, sum, v int64
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
		sum += v
	}
	require.NoError(t, rows.Err())
	require.Equal(t, int64(1000000), n)
	require.Equal(t, int64(499999500000), sum)
}

// A streaming query that fails part-way through must report the failure, not end early.
func TestStreamingResultsReportsAFailurePartWay(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()), failPartWayQuery)
	require.NoError(t, err, "the failure is past the first chunk, so it surfaces while fetching")
	defer closeRowsWrapper(t, rows)
	var n int
	var v string
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
	}
	require.ErrorContains(t, rows.Err(), "boom")
	require.Positive(t, n)
	require.Less(t, n, 100000)
}

func TestQueryWithoutStreamingReportsAFailureUpFront(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.QueryContext(context.Background(), failPartWayQuery)
	require.ErrorContains(t, err, "boom")
}

const (
	// failPartWayQuery fails after its first chunks. DuckDB buffers a streaming result
	// ahead of the reader by size, so its rows are wide enough to fill that buffer first.
	failPartWayQuery = `SELECT CASE WHEN i = 100000 THEN error('boom') ELSE repeat('x', 1000) END FROM range(200000) t(i)`
	// endlessQuery produces its first row quickly, and does not finish in any test's lifetime.
	endlessQuery = `SELECT i FROM range(1000000000000) t(i)`
	// endlessCountQuery runs for longer than any test's lifetime before producing a row.
	endlessCountQuery = `SELECT count(*) FROM range(1000000000000) t(i)`
)

// streamPath runs a query through one of the driver's query paths, reads at most limit rows
// of its first column (all if limit < 0), calls beforeClose if not nil, and closes the rows.
// nextErr is the error that ended the reading, if any.
type streamPath struct {
	name string
	run  func(t *testing.T, c *sql.Conn, ctx context.Context, query string, limit int, beforeClose func()) (vals []any, nextErr, closeErr error)
}

var streamPaths = []streamPath{
	{name: "QueryContext", run: func(t *testing.T, c *sql.Conn, ctx context.Context, query string, limit int, beforeClose func()) ([]any, error, error) {
		rows, err := c.QueryContext(ctx, query)
		require.NoError(t, err)
		return readSQLRows(t, rows, limit, beforeClose)
	}},
	{name: "prepared QueryContext", run: func(t *testing.T, c *sql.Conn, ctx context.Context, query string, limit int, beforeClose func()) ([]any, error, error) {
		stmt, err := c.PrepareContext(context.Background(), query)
		require.NoError(t, err)
		defer closePreparedWrapper(t, stmt)
		rows, err := stmt.QueryContext(ctx)
		require.NoError(t, err)
		return readSQLRows(t, rows, limit, beforeClose)
	}},
	{name: "QueryBound", run: func(t *testing.T, c *sql.Conn, ctx context.Context, query string, limit int, beforeClose func()) ([]any, error, error) {
		var vals []any
		var nextErr, closeErr error
		require.NoError(t, c.Raw(func(driverConn any) error {
			s, err := driverConn.(*Conn).PrepareContext(context.Background(), query)
			require.NoError(t, err)
			stmt := s.(*Stmt)
			defer func() { require.NoError(t, stmt.Close()) }()
			require.NoError(t, stmt.Bind(nil))
			rows, err := stmt.QueryBound(ctx)
			require.NoError(t, err)
			dst := make([]driver.Value, len(rows.Columns()))
			for limit < 0 || len(vals) < limit {
				if err = rows.Next(dst); err != nil {
					if !errors.Is(err, io.EOF) {
						nextErr = err
					}
					break
				}
				vals = append(vals, dst[0])
			}
			if beforeClose != nil {
				beforeClose()
			}
			closeErr = rows.Close()
			return nil
		}))
		return vals, nextErr, closeErr
	}},
}

func readSQLRows(t *testing.T, rows *sql.Rows, limit int, beforeClose func()) ([]any, error, error) {
	var vals []any
	for (limit < 0 || len(vals) < limit) && rows.Next() {
		var v any
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	nextErr := rows.Err()
	if beforeClose != nil {
		beforeClose()
	}
	closeErr := rows.Close()
	if closeErr == nil && nextErr == nil {
		// If beforeClose canceled the query's context, database/sql closes the rows on its own
		// goroutine, which may get there before this Close. That Close then returns nil, and
		// Err reports the cancellation instead.
		closeErr = rows.Err()
	}
	return vals, nextErr, closeErr
}

type streamingCtxKey struct{}

// ctxValueUDF returns the streamingCtxKey value of the context it runs with.
type ctxValueUDF struct{}

func (*ctxValueUDF) Config() ScalarFuncConfig {
	in, _ := NewTypeInfo(TYPE_BIGINT)
	out, _ := NewTypeInfo(TYPE_VARCHAR)
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{in}, ResultTypeInfo: out, Volatile: true}
}

func (*ctxValueUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, _ []driver.Value) (any, error) {
		v, _ := ctx.Value(streamingCtxKey{}).(string)
		return v, nil
	}}
}

// A streaming query runs its UDFs while it is fetched, so they must still see its context.
func TestStreamingResultsKeepUDFContext(t *testing.T) {
	for _, path := range streamPaths {
		t.Run(path.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			c := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, c)
			require.NoError(t, RegisterScalarUDF(c, "ctx_value", &ctxValueUDF{}))

			ctx := WithStreamingResults(context.WithValue(context.Background(), streamingCtxKey{}, "query"))
			vals, nextErr, closeErr := path.run(t, c, ctx, `SELECT ctx_value(i) FROM range(100000) t(i)`, -1, nil)
			require.NoError(t, nextErr)
			require.NoError(t, closeErr)
			require.Len(t, vals, 100000)
			for _, v := range vals {
				require.Equal(t, "query", v)
			}
		})
	}
}

// Cancellation must interrupt a streaming query while it is fetched, not only while it starts.
func TestStreamingResultsCancelInterruptsFetch(t *testing.T) {
	for _, path := range streamPaths {
		t.Run(path.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			c := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, c)

			ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
			defer cancel()
			time.AfterFunc(100*time.Millisecond, cancel)

			start := time.Now()
			_, nextErr, closeErr := path.run(t, c, ctx, endlessCountQuery, -1, nil)
			require.ErrorIs(t, nextErr, context.Canceled)
			require.NoError(t, closeErr)
			require.Less(t, time.Since(start), 10*time.Second)
		})
	}
}

// Closing streaming rows must stop their interrupter, before the connection runs other statements.
func TestStreamingResultsConnectionReuse(t *testing.T) {
	patchVar(t, &interruptInterval, 10*time.Millisecond)
	var interrupts atomic.Int64
	interrupt := mapping.Interrupt
	patchVar(t, &mapping.Interrupt, func(conn mapping.Connection) {
		interrupts.Add(1)
		interrupt(conn)
	})

	for _, path := range streamPaths {
		t.Run(path.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			c := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, c)

			for _, limit := range []int{-1, 1} {
				ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
				vals, nextErr, closeErr := path.run(t, c, ctx, `SELECT i FROM range(100000) t(i)`, limit, nil)
				require.NoError(t, nextErr)
				require.NoError(t, closeErr)
				require.NotEmpty(t, vals)

				interrupts.Store(0)
				cancel()
				var n int64
				require.NoError(t, c.QueryRowContext(context.Background(), `SELECT count(*) FROM range(10000000)`).Scan(&n))
				require.Equal(t, int64(10000000), n)
				time.Sleep(5 * interruptInterval)
				require.Zero(t, interrupts.Load(), "a closed streaming query must not interrupt its connection")
			}
		})
	}
}

// Closing streaming rows early must run the rest of the query, so that its writes are not lost.
func TestStreamingResultsEarlyCloseRunsTheRest(t *testing.T) {
	ctx := WithStreamingResults(context.Background())

	t.Run("autocommit", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)

		var v int64
		require.NoError(t, db.QueryRowContext(ctx,
			`WITH ins AS (INSERT INTO t SELECT i FROM range(100000) r(i) RETURNING i) SELECT i FROM ins`).Scan(&v))
		require.Equal(t, int64(100000), countTableRows(t, db))
	})

	t.Run("transaction", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)
		createTable(t, db, `CREATE SEQUENCE s`)

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
		require.NoError(t, err)
		var v int64
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT nextval('s')`).Scan(&v))
		require.Equal(t, int64(1), v)
		require.NoError(t, tx.Commit())

		require.Equal(t, int64(1), countTableRows(t, db))
		require.NoError(t, db.QueryRow(`SELECT nextval('s')`).Scan(&v))
		require.Equal(t, int64(2), v)
	})

	for _, path := range streamPaths {
		t.Run(path.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			createTable(t, db, `CREATE TABLE t(i BIGINT)`)
			c := openConnWrapper(t, db, ctx)
			defer closeConnWrapper(t, c)

			vals, nextErr, closeErr := path.run(t, c, ctx,
				`WITH ins AS (INSERT INTO t SELECT i FROM range(100000) r(i) RETURNING i) SELECT i FROM ins`, 1, nil)
			require.NoError(t, nextErr)
			require.NoError(t, closeErr)
			require.Len(t, vals, 1)
			require.Equal(t, int64(100000), countTableRows(t, db))
		})
	}
}

// A failure in the unread rest of a streaming query must be reported by Close.
func TestStreamingResultsEarlyCloseReportsAFailure(t *testing.T) {
	ctx := WithStreamingResults(context.Background())

	for _, path := range streamPaths {
		t.Run(path.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			c := openConnWrapper(t, db, ctx)
			defer closeConnWrapper(t, c)

			vals, nextErr, closeErr := path.run(t, c, ctx, failPartWayQuery, 1, nil)
			require.Len(t, vals, 1)
			require.NoError(t, nextErr)
			require.ErrorContains(t, closeErr, "boom")
		})
	}

	t.Run("Rows.Err", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)

		rows, err := db.QueryContext(ctx, failPartWayQuery)
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.ErrorContains(t, rows.Close(), "boom")
		require.ErrorContains(t, rows.Err(), "boom")
	})

	t.Run("QueryRow.Scan", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)

		var v string
		require.ErrorContains(t, db.QueryRowContext(ctx, failPartWayQuery).Scan(&v), "boom")
	})

	t.Run("explicit close in transaction", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
		require.NoError(t, err)
		var v string
		require.ErrorContains(t, tx.QueryRowContext(ctx, failPartWayQuery).Scan(&v), "boom")
		err = tx.Commit()
		require.ErrorIs(t, err, errCommit)
		require.ErrorContains(t, err, "boom")
		require.Zero(t, countTableRows(t, db))
	})

	// Commit closes open rows itself, and database/sql drops the error of that close.
	t.Run("Commit closes the rows", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
		require.NoError(t, err)
		rows, err := tx.QueryContext(ctx, failPartWayQuery)
		require.NoError(t, err)
		require.True(t, rows.Next())

		err = tx.Commit()
		require.ErrorIs(t, err, errCommit)
		require.ErrorContains(t, err, "boom")
		require.Zero(t, countTableRows(t, db))

		// The connection works after the failed commit.
		tx, err = db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (2)`)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, int64(1), countTableRows(t, db))
	})
}

// Canceling the query or its transaction abandons the unread rest of a streaming query.
func TestStreamingResultsCancelAbandonsTheRest(t *testing.T) {
	t.Run("query context", func(t *testing.T) {
		for _, path := range streamPaths {
			t.Run(path.name, func(t *testing.T) {
				db := openDbWrapper(t, ``)
				defer closeDbWrapper(t, db)
				c := openConnWrapper(t, db, context.Background())
				defer closeConnWrapper(t, c)

				ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
				defer cancel()

				start := time.Now()
				// Draining would never end, so a timely Close shows that it abandoned the query.
				// Close reports the cancellation, as the unread work did not run.
				vals, _, closeErr := path.run(t, c, ctx, endlessQuery, 1, cancel)
				require.Len(t, vals, 1)
				require.ErrorIs(t, closeErr, context.Canceled)
				require.Less(t, time.Since(start), 10*time.Second)
			})
		}
	})

	t.Run("transaction context", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)

		txCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tx, err := db.BeginTx(txCtx, nil)
		require.NoError(t, err)
		rows, err := tx.QueryContext(WithStreamingResults(context.Background()), endlessQuery)
		require.NoError(t, err)
		require.True(t, rows.Next())

		start := time.Now()
		cancel()
		// database/sql closes the rows and rolls back once the transaction context is canceled.
		require.Eventually(t, func() bool { return errors.Is(tx.Commit(), sql.ErrTxDone) }, 10*time.Second, 10*time.Millisecond)
		// database/sql closes the rows on its own goroutine, which may not have run yet when Commit
		// reports ErrTxDone. If it has, Close returns nil; if not, this Close abandons the rest and
		// reports the cancellation. Either way, Err reports it.
		if err := rows.Close(); err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		require.ErrorIs(t, rows.Err(), context.Canceled)
		require.Less(t, time.Since(start), 10*time.Second)
	})
}

// While streaming rows are open, the query occupies its connection.
func TestStreamingResultsOccupyTheConnection(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	tx, err := db.Begin()
	require.NoError(t, err)
	rows, err := tx.QueryContext(WithStreamingResults(context.Background()), `SELECT i FROM range(100000) t(i)`)
	require.NoError(t, err)
	require.True(t, rows.Next())

	_, err = tx.Exec(`SELECT 1`)
	require.Error(t, err)

	var n int
	for rows.Next() {
		n++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 99999, n)
	require.NoError(t, rows.Close())

	_, err = tx.Exec(`SELECT 1`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// Commit and Rollback must end the transaction even if a streaming result is still open that
// database/sql does not close for them, such as rows queried through the sql.Conn of the
// transaction. DuckDB would otherwise reject the statement, and leave the transaction open.
func TestStreamingResultsOpenAtTxEnd(t *testing.T) {
	// setup returns a transaction with a pending insert, and open streaming rows of query
	// on the connection of that transaction.
	setup := func(t *testing.T, query string) (*sql.DB, *sql.Conn, *sql.Tx, *sql.Rows) {
		t.Helper()
		db := openDbWrapper(t, ``)
		t.Cleanup(func() { closeDbWrapper(t, db) })
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)
		createTable(t, db, `CREATE SEQUENCE s`)
		c := openConnWrapper(t, db, context.Background())
		t.Cleanup(func() { closeConnWrapper(t, c) })

		tx, err := c.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
		require.NoError(t, err)
		rows, err := c.QueryContext(WithStreamingResults(context.Background()), query)
		require.NoError(t, err)
		require.True(t, rows.Next())
		return db, c, tx, rows
	}
	// requireEnded checks that the rows report the end of the transaction, not the end of
	// the result, as they are not complete.
	requireEnded := func(t *testing.T, rows *sql.Rows) {
		t.Helper()
		for rows.Next() {
		}
		require.ErrorIs(t, rows.Err(), errStreamTxEnded)
		require.NoError(t, rows.Close())
	}
	// requireNextTx checks that the connection can run another transaction.
	requireNextTx := func(t *testing.T, c *sql.Conn) {
		t.Helper()
		tx, err := c.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (2)`)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	t.Run("Commit runs the rest", func(t *testing.T) {
		db, c, tx, rows := setup(t, `SELECT nextval('s') FROM range(100000) t(i)`)
		require.NoError(t, tx.Commit())
		require.Equal(t, int64(1), countTableRows(t, db))
		var v int64
		require.NoError(t, db.QueryRow(`SELECT nextval('s')`).Scan(&v))
		require.Equal(t, int64(100001), v)

		requireEnded(t, rows)
		requireNextTx(t, c)
		require.Equal(t, int64(2), countTableRows(t, db))
	})

	// Rows that were all fetched are complete, although they were still open at Commit.
	t.Run("Commit with no rows left", func(t *testing.T) {
		db, c, tx, rows := setup(t, `SELECT i FROM range(100) t(i)`)
		require.NoError(t, tx.Commit())

		n := 1
		for rows.Next() {
			n++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 100, n)
		require.NoError(t, rows.Close())
		requireNextTx(t, c)
		require.Equal(t, int64(2), countTableRows(t, db))
	})

	t.Run("Commit fails if the rest fails", func(t *testing.T) {
		db, c, tx, rows := setup(t, failPartWayQuery)
		err := tx.Commit()
		require.ErrorIs(t, err, errCommit)
		require.ErrorContains(t, err, "boom")
		require.Zero(t, countTableRows(t, db))

		for rows.Next() {
		}
		require.ErrorContains(t, rows.Err(), "boom")
		require.NoError(t, rows.Close())
		requireNextTx(t, c)
		require.Equal(t, int64(1), countTableRows(t, db))
	})

	t.Run("Rollback abandons the rest", func(t *testing.T) {
		db, c, tx, rows := setup(t, endlessQuery)
		require.NoError(t, tx.Rollback())
		require.Zero(t, countTableRows(t, db))

		requireEnded(t, rows)
		requireNextTx(t, c)
		require.Equal(t, int64(1), countTableRows(t, db))
	})
}

// countTableRows returns the number of rows in table t.
func countTableRows(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM t`).Scan(&n))
	return n
}

// Canceling the transaction context must interrupt a streaming query that runs under a
// separate query context, both while it is fetched and while Close runs the rest of it.
func TestStreamingResultsTxCancelInterrupts(t *testing.T) {
	t.Run("in-flight Next", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)

		txCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tx, err := db.BeginTx(txCtx, nil)
		require.NoError(t, err)
		rows, err := tx.QueryContext(WithStreamingResults(context.Background()), endlessCountQuery)
		require.NoError(t, err)

		time.AfterFunc(100*time.Millisecond, cancel)
		start := time.Now()
		require.False(t, rows.Next())
		require.ErrorIs(t, rows.Err(), context.Canceled)
		require.Less(t, time.Since(start), 10*time.Second)
		require.ErrorIs(t, tx.Commit(), sql.ErrTxDone)
	})

	t.Run("close-time drain", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)

		txCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tx, err := db.BeginTx(txCtx, nil)
		require.NoError(t, err)
		rows, err := tx.QueryContext(WithStreamingResults(context.Background()), endlessQuery)
		require.NoError(t, err)
		require.True(t, rows.Next())

		time.AfterFunc(100*time.Millisecond, cancel)
		start := time.Now()
		require.ErrorIs(t, rows.Close(), context.Canceled)
		require.Less(t, time.Since(start), 10*time.Second)
		require.ErrorIs(t, tx.Commit(), sql.ErrTxDone)
	})

	t.Run("driver", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		c := openConnWrapper(t, db, context.Background())
		defer closeConnWrapper(t, c)

		for _, query := range []string{endlessCountQuery, endlessQuery} {
			require.NoError(t, c.Raw(func(driverConn any) error {
				conn := driverConn.(*Conn)
				txCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tx, err := conn.BeginTx(txCtx, driver.TxOptions{})
				require.NoError(t, err)
				defer func() { require.NoError(t, tx.Rollback()) }()

				s, err := conn.PrepareContext(context.Background(), query)
				require.NoError(t, err)
				stmt := s.(*Stmt)
				defer func() { require.NoError(t, stmt.Close()) }()
				rows, err := stmt.QueryContext(WithStreamingResults(context.Background()), nil)
				require.NoError(t, err)

				time.AfterFunc(100*time.Millisecond, cancel)
				start := time.Now()
				dst := make([]driver.Value, 1)
				var closeErr error
				if err = rows.Next(dst); err == nil {
					// endlessQuery: the first row arrives, and Close runs the rest.
					closeErr = rows.Close()
				} else {
					closeErr = rows.Close()
					require.ErrorIs(t, err, context.Canceled)
				}
				require.ErrorIs(t, errors.Join(err, closeErr), context.Canceled)
				require.Less(t, time.Since(start), 10*time.Second)
				return nil
			}))
		}
	})
}

// Canceling while Close runs the rest of a streaming query abandons its writes,
// so QueryRow.Scan and Commit must report the cancellation, not succeed.
func TestStreamingResultsCancelDuringDrainIsReported(t *testing.T) {
	const nextvalQuery = `SELECT nextval('s') FROM range(1000000000000) t(i)`

	t.Run("autocommit", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE SEQUENCE s`)

		ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
		defer cancel()
		time.AfterFunc(200*time.Millisecond, cancel)
		var v int64
		require.ErrorIs(t, db.QueryRowContext(ctx, nextvalQuery).Scan(&v), context.Canceled)
	})

	t.Run("transaction", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		createTable(t, db, `CREATE TABLE t(i BIGINT)`)
		createTable(t, db, `CREATE SEQUENCE s`)

		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
		defer cancel()
		time.AfterFunc(200*time.Millisecond, cancel)
		var v int64
		require.ErrorIs(t, tx.QueryRowContext(ctx, nextvalQuery).Scan(&v), context.Canceled)

		err = tx.Commit()
		require.ErrorIs(t, err, errCommit)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, countTableRows(t, db))
	})
}

// nestedQueryUDF runs a query on another connection with the context it is given, once,
// and returns its argument.
type nestedQueryUDF struct {
	db   *sql.DB
	done atomic.Bool
	err  atomic.Value
}

func (*nestedQueryUDF) Config() ScalarFuncConfig {
	in, _ := NewTypeInfo(TYPE_BIGINT)
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{in}, ResultTypeInfo: in, Volatile: true}
}

func (u *nestedQueryUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
		if u.done.CompareAndSwap(false, true) {
			var v int64
			if err := u.db.QueryRowContext(ctx, `SELECT 42`).Scan(&v); err != nil {
				u.err.Store(err)
			}
		}
		return values[0], nil
	}}
}

// A UDF may pass its context to a query on another connection.
// Closing that query must not end the scope of the outer streaming query.
func TestStreamingResultsNestedQueryInUDF(t *testing.T) {
	setup := func(t *testing.T) (*sql.DB, *sql.Conn, *nestedQueryUDF) {
		db := openDbWrapper(t, ``)
		t.Cleanup(func() { closeDbWrapper(t, db) })
		c := openConnWrapper(t, db, context.Background())
		t.Cleanup(func() { closeConnWrapper(t, c) })
		udf := &nestedQueryUDF{db: db}
		require.NoError(t, RegisterScalarUDF(c, "nested_query", udf))
		return db, c, udf
	}

	t.Run("cancel still interrupts the outer query", func(t *testing.T) {
		_, c, udf := setup(t)
		ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
		defer cancel()
		rows, err := c.QueryContext(ctx, `SELECT count(*) FROM range(1000000000000) t(i) WHERE i >= (SELECT nested_query(0))`)
		require.NoError(t, err)

		time.AfterFunc(200*time.Millisecond, cancel)
		start := time.Now()
		require.False(t, rows.Next())
		require.ErrorIs(t, rows.Err(), context.Canceled)
		require.NoError(t, rows.Close())
		require.Less(t, time.Since(start), 10*time.Second)
		require.True(t, udf.done.Load())
		require.Nil(t, udf.err.Load())
	})

	t.Run("transaction outer query is not canceled", func(t *testing.T) {
		db, _, udf := setup(t)
		// A cancelable transaction context, so that the query links to it.
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(WithStreamingResults(context.Background()), `SELECT nested_query(i) FROM range(100000) t(i)`)
		require.NoError(t, err)
		n := 0
		for rows.Next() {
			n++
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 100000, n)
		require.NoError(t, tx.Commit())
		require.True(t, udf.done.Load())
		require.Nil(t, udf.err.Load())
	})
}

// savedCtxUDF keeps the first context that it runs with, and returns its argument.
type savedCtxUDF struct {
	ctx atomic.Pointer[context.Context]
}

func (*savedCtxUDF) Config() ScalarFuncConfig {
	in, _ := NewTypeInfo(TYPE_BIGINT)
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{in}, ResultTypeInfo: in, Volatile: true}
}

func (u *savedCtxUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
		u.ctx.CompareAndSwap(nil, &ctx)
		return values[0], nil
	}}
}

// A UDF may keep its context and use it after the query is closed, as it could the caller's
// context. Closing the query must not cancel it, also in a transaction, and canceling it must
// still interrupt a later query on the same connection.
func TestStreamingResultsSavedUDFContext(t *testing.T) {
	for _, inTx := range []bool{false, true} {
		for _, path := range streamPaths {
			name := path.name
			if inTx {
				name = "transaction " + name
			}
			t.Run(name, func(t *testing.T) {
				db := openDbWrapper(t, ``)
				defer closeDbWrapper(t, db)
				c := openConnWrapper(t, db, context.Background())
				defer closeConnWrapper(t, c)
				udf := &savedCtxUDF{}
				require.NoError(t, RegisterScalarUDF(c, "saved_ctx", udf))

				var tx *sql.Tx
				if inTx {
					// A cancelable transaction context, so that the query links to it.
					var err error
					tx, err = c.BeginTx(t.Context(), nil)
					require.NoError(t, err)
				}

				ctx, cancel := context.WithCancel(WithStreamingResults(context.WithValue(context.Background(), streamingCtxKey{}, "query")))
				defer cancel()
				vals, nextErr, closeErr := path.run(t, c, ctx, `SELECT saved_ctx(i) FROM range(10) t(i)`, -1, nil)
				require.NoError(t, nextErr)
				require.NoError(t, closeErr)
				require.Len(t, vals, 10)
				if tx != nil {
					require.NoError(t, tx.Commit())
				}

				require.NotNil(t, udf.ctx.Load())
				saved := *udf.ctx.Load()
				require.Equal(t, "query", saved.Value(streamingCtxKey{}))
				require.NoError(t, saved.Err(), "closing the query must not cancel the UDF context")

				time.AfterFunc(100*time.Millisecond, cancel)
				start := time.Now()
				_, err := c.ExecContext(saved, endlessCountQuery)
				require.ErrorIs(t, err, context.Canceled)
				require.Less(t, time.Since(start), 10*time.Second)
			})
		}
	}
}

// Closing a connection before its streaming rows must stop the interrupter, which would
// otherwise interrupt the freed connection once the query's context is canceled.
// The rows then report that the rest of the query did not run.
func TestStreamingResultsConnCloseBeforeRows(t *testing.T) {
	patchVar(t, &interruptInterval, 10*time.Millisecond)

	for _, inTx := range []bool{false, true} {
		for _, readOn := range []bool{false, true} {
			t.Run(fmt.Sprintf("tx=%v/readOn=%v", inTx, readOn), func(t *testing.T) {
				c := newConnectorWrapper(t, ``, nil)
				defer closeConnectorWrapper(t, c)
				conn := openDriverConnWrapper(t, c).(*Conn)

				ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
				defer cancel()
				if inTx {
					_, err := conn.BeginTx(ctx, driver.TxOptions{})
					require.NoError(t, err)
				}
				rows, err := conn.QueryContext(ctx, endlessQuery, nil)
				require.NoError(t, err)
				dst := make([]driver.Value, 1)
				require.NoError(t, rows.Next(dst))

				require.NoError(t, conn.Close())
				cancel()
				time.Sleep(5 * interruptInterval)

				if !readOn {
					require.ErrorIs(t, rows.Close(), errClosedCon)
					return
				}
				// Rows of the current chunk were already fetched; the next fetch fails.
				for err = rows.Next(dst); err == nil; err = rows.Next(dst) {
				}
				require.ErrorIs(t, err, errClosedCon)
				require.NoError(t, rows.Close(), "Next already reported the failure")
			})
		}
	}
}

// DuckDB does not stream every result, even under WithStreamingResults. Such a result is
// complete when the query returns, so canceling its context must not interrupt other
// statements on the connection while its rows are open.
func TestStreamingResultsNotStreamedResult(t *testing.T) {
	patchVar(t, &interruptInterval, 10*time.Millisecond)
	const slow = `SELECT sum(hash(i)) FROM range(50000000) t(i)`

	for _, q := range []string{
		`INSERT INTO t SELECT i FROM range(10) r(i) RETURNING i`,
		`EXPLAIN SELECT 42`,
	} {
		for _, inTx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tx=%v", strings.Fields(q)[0], inTx), func(t *testing.T) {
				db := openDbWrapper(t, ``)
				defer closeDbWrapper(t, db)
				c := openConnWrapper(t, db, context.Background())
				defer closeConnWrapper(t, c)
				_, err := c.ExecContext(context.Background(), `CREATE TABLE t (i BIGINT)`)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
				defer cancel()
				var rows *sql.Rows
				exec := func() error { _, err := c.ExecContext(context.Background(), slow); return err }
				if inTx {
					tx, err := c.BeginTx(context.Background(), nil)
					require.NoError(t, err)
					defer func() { require.NoError(t, tx.Rollback()) }()
					rows, err = tx.QueryContext(ctx, q)
					require.NoError(t, err)
					exec = func() error { _, err := tx.ExecContext(context.Background(), slow); return err }
				} else {
					rows, err = c.QueryContext(ctx, q)
					require.NoError(t, err)
				}
				require.True(t, rows.Next())

				// The statement runs while the rows are open: it holds the connection,
				// so database/sql closes the rows only after it.
				errCh := make(chan error, 1)
				go func() { errCh <- exec() }()
				time.Sleep(20 * time.Millisecond)
				cancel()
				require.NoError(t, <-errCh)
				require.NoError(t, rows.Close())
			})
		}
	}
}

// Driver rows of a streaming query, used without database/sql, are complete once Next
// reports the end of the result or a failed fetch. Their connection can then run other
// statements before the rows are closed, and canceling the query's context must not
// interrupt them. database/sql closes the rows at that point anyway.
func TestStreamingResultsEndBeforeClose(t *testing.T) {
	patchVar(t, &interruptInterval, 10*time.Millisecond)
	const slow = `SELECT sum(hash(i)) FROM range(50000000) t(i)`

	for _, tc := range []struct {
		name, query, err string
	}{
		{name: "eof", query: `SELECT i FROM range(10000) t(i)`},
		{name: "fetch error", query: failPartWayQuery, err: "boom"},
	} {
		for _, inTx := range []bool{false, true} {
			if inTx && tc.err != "" {
				// A failed statement aborts its transaction, so no other statement can run in it.
				continue
			}
			t.Run(fmt.Sprintf("%s/tx=%v", tc.name, inTx), func(t *testing.T) {
				c := newConnectorWrapper(t, ``, nil)
				defer closeConnectorWrapper(t, c)
				conn := openDriverConnWrapper(t, c).(*Conn)
				defer func() { require.NoError(t, conn.Close()) }()

				if inTx {
					tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
					require.NoError(t, err)
					defer func() { require.NoError(t, tx.Rollback()) }()
				}

				ctx, cancel := context.WithCancel(WithStreamingResults(context.Background()))
				defer cancel()
				rows, err := conn.QueryContext(ctx, tc.query, nil)
				require.NoError(t, err)
				dst := make([]driver.Value, len(rows.Columns()))
				for err == nil {
					err = rows.Next(dst)
				}
				if tc.err == "" {
					require.ErrorIs(t, err, io.EOF)
				} else {
					require.ErrorContains(t, err, tc.err)
				}

				errCh := make(chan error, 1)
				go func() {
					_, err := conn.ExecContext(context.Background(), slow, nil)
					errCh <- err
				}()
				time.Sleep(20 * time.Millisecond)
				cancel()
				require.NoError(t, <-errCh)

				// The rows still report how the result ended, and closing them reports no failure:
				// the failure was already returned by Next.
				err = rows.Next(dst)
				if tc.err == "" {
					require.ErrorIs(t, err, io.EOF)
				} else {
					require.ErrorContains(t, err, tc.err)
				}
				require.NoError(t, rows.Close())
			})
		}
	}
}
