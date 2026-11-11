package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin down which context a scalar UDF executor receives. DuckDB reuses one bind data
// instance for every execution of a prepared statement, so the executor must see the context of the
// statement that is executing it, never the context of the statement that prepared, bound, or first
// executed the plan. Each test prepares under a context that is canceled before execution starts,
// which is how a server that scopes contexts per protocol message prepares statements.

type execCtxStmtKey struct{}

type execCtxBinderKey struct{}

// execCtxSink collects rejections recorded through the context, like an application that reports
// structured errors next to the driver error.
type execCtxSink struct {
	mu   sync.Mutex
	errs []string
}

func (s *execCtxSink) record(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, msg)
}

func (s *execCtxSink) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.errs...)
}

type execCtxSinkKey struct{}

// execCtxUDF is a configurable scalar UDF for the context tests.
type execCtxUDF struct {
	config   ScalarFuncConfig
	executor ScalarFuncExecutor
}

func (u *execCtxUDF) Config() ScalarFuncConfig     { return u.config }
func (u *execCtxUDF) Executor() ScalarFuncExecutor { return u.executor }

func execCtxBigintToVarchar(t *testing.T) ScalarFuncConfig {
	t.Helper()
	bigint, err := NewTypeInfo(TYPE_BIGINT)
	require.NoError(t, err)
	varchar, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{bigint}, ResultTypeInfo: varchar}
}

// execCtxValue renders what the executor can see in its context.
func execCtxValue(ctx context.Context) string {
	stmt, _ := ctx.Value(execCtxStmtKey{}).(string)
	binder, _ := ctx.Value(execCtxBinderKey{}).(string)
	if binder == "" {
		return stmt
	}
	return stmt + "|" + binder
}

func execCtxRowExecutor(ctx context.Context, values []driver.Value) (any, error) {
	if n, ok := values[0].(int64); ok && n < 0 {
		if sink, ok := ctx.Value(execCtxSinkKey{}).(*execCtxSink); ok {
			sink.record(fmt.Sprintf("rejected %d", n))
		}
		return nil, fmt.Errorf("rejected %d", n)
	}
	return execCtxValue(ctx), nil
}

func execCtxChunkExecutor(ctx context.Context, chunk *ChunkIteratorState) error {
	for row, err := range chunk.Rows() {
		if err != nil {
			return err
		}
		if err := row.SetResult(execCtxValue(ctx)); err != nil {
			return err
		}
	}
	return nil
}

// prepareCanceled prepares query under a context carrying stmt "prepare", then cancels it.
func prepareCanceled(t *testing.T, conn *sql.Conn, query string) *sql.Stmt {
	t.Helper()
	prepCtx, cancel := context.WithCancel(context.WithValue(context.Background(), execCtxStmtKey{}, "prepare"))
	stmt, err := conn.PrepareContext(prepCtx, query)
	cancel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close() })
	return stmt
}

// execStatement runs one execution under its own context and cancels that context afterwards.
func execStatement(stmt *sql.Stmt, stmtValue string, sink *execCtxSink) ([]string, error) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), execCtxStmtKey{}, stmtValue))
	defer cancel()
	if sink != nil {
		ctx = context.WithValue(ctx, execCtxSinkKey{}, sink)
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v.String)
	}
	return out, rows.Err()
}

func TestScalarUDFExecutionContextOnReExecution(t *testing.T) {
	tests := []struct {
		name     string
		executor ScalarFuncExecutor
	}{
		{name: "row executor", executor: ScalarFuncExecutor{RowContextExecutor: execCtxRowExecutor}},
		{name: "chunk executor", executor: ScalarFuncExecutor{ChunkContextExecutor: execCtxChunkExecutor}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			conn := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, conn)
			require.NoError(t, RegisterScalarUDF(conn, "exec_ctx", &execCtxUDF{config: execCtxBigintToVarchar(t), executor: tt.executor}))

			// A column argument keeps the call out of constant folding, so it runs at execution.
			stmt := prepareCanceled(t, conn, `SELECT exec_ctx(i) FROM range(3) t(i)`)
			for n := 1; n <= 3; n++ {
				want := fmt.Sprintf("exec-%d", n)
				got, err := execStatement(stmt, want, nil)
				require.NoError(t, err, "execution %d", n)
				require.Equal(t, []string{want, want, want}, got, "execution %d must see its own context", n)
			}
		})
	}
}

func TestScalarUDFExecutionContextAfterConstantFolding(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)
	require.NoError(t, RegisterScalarUDF(conn, "exec_ctx", &execCtxUDF{
		config:   execCtxBigintToVarchar(t),
		executor: ScalarFuncExecutor{RowContextExecutor: execCtxRowExecutor},
	}))

	// All-constant arguments: the optimizer evaluates the call while preparing. A rejection there is
	// swallowed and the call runs again at execution, which must report the function's own error and
	// record it in the executing statement's sink.
	stmt := prepareCanceled(t, conn, `SELECT exec_ctx(-1)`)
	for n := 1; n <= 2; n++ {
		sink := &execCtxSink{}
		_, err := execStatement(stmt, fmt.Sprintf("exec-%d", n), sink)
		require.Error(t, err)
		require.Contains(t, err.Error(), "rejected -1", "execution %d", n)
		require.NotContains(t, err.Error(), context.Canceled.Error(), "execution %d", n)
		require.Equal(t, []string{"rejected -1"}, sink.recorded(), "execution %d", n)
	}

	// An accepted all-constant call is folded while preparing and does not run again.
	folded := prepareCanceled(t, conn, `SELECT exec_ctx(1)`)
	got, err := execStatement(folded, "exec", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"prepare"}, got, "a folded call keeps the value computed while preparing")
}

func TestScalarUDFExecutionContextWithBinder(t *testing.T) {
	tests := []struct {
		name   string
		binder ScalarBinderFn
		want   func(n int) string
	}{
		{
			name:   "binder returns its parent",
			binder: func(parent context.Context, _ []ScalarUDFArg) (context.Context, error) { return parent, nil },
			want:   func(n int) string { return fmt.Sprintf("exec-%d", n) },
		},
		{
			name:   "binder returns nil",
			binder: func(context.Context, []ScalarUDFArg) (context.Context, error) { return nil, nil },
			want:   func(n int) string { return fmt.Sprintf("exec-%d", n) },
		},
		{
			name: "binder adds a value",
			binder: func(parent context.Context, _ []ScalarUDFArg) (context.Context, error) {
				return context.WithValue(parent, execCtxBinderKey{}, "bound"), nil
			},
			want: func(n int) string { return fmt.Sprintf("exec-%d|bound", n) },
		},
		{
			name: "binder overrides a statement value",
			binder: func(parent context.Context, _ []ScalarUDFArg) (context.Context, error) {
				return context.WithValue(parent, execCtxStmtKey{}, "bound"), nil
			},
			// The executing statement wins for keys it carries itself.
			want: func(n int) string { return fmt.Sprintf("exec-%d", n) },
		},
		{
			name: "binder derives a cancelable child",
			binder: func(parent context.Context, _ []ScalarUDFArg) (context.Context, error) {
				child, cancel := context.WithCancel(context.WithValue(parent, execCtxBinderKey{}, "bound"))
				cancel() // Canceling the child must not fail later executions either.
				return child, nil
			},
			want: func(n int) string { return fmt.Sprintf("exec-%d|bound", n) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)
			conn := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, conn)
			require.NoError(t, RegisterScalarUDF(conn, "exec_ctx", &execCtxUDF{
				config:   execCtxBigintToVarchar(t),
				executor: ScalarFuncExecutor{RowContextExecutor: execCtxRowExecutor, ScalarBinder: tt.binder},
			}))

			stmt := prepareCanceled(t, conn, `SELECT exec_ctx(i) FROM range(2) t(i)`)
			for n := 1; n <= 3; n++ {
				got, err := execStatement(stmt, fmt.Sprintf("exec-%d", n), nil)
				require.NoError(t, err, "execution %d", n)
				require.Equal(t, []string{tt.want(n), tt.want(n)}, got, "execution %d", n)
			}
		})
	}
}

func TestScalarUDFExecutionContextParallel(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var calls atomic.Int64
	require.NoError(t, RegisterScalarUDF(conn, "exec_ctx", &execCtxUDF{
		config: execCtxBigintToVarchar(t),
		executor: ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
			calls.Add(1)
			return execCtxValue(ctx), nil
		}},
	}))
	_, err := conn.ExecContext(context.Background(), `SET threads = 8`)
	require.NoError(t, err)
	// Several row groups, so that the scan and the function run on several worker threads.
	const n = 1_000_000
	_, err = conn.ExecContext(context.Background(), fmt.Sprintf(`CREATE TABLE exec_ctx_rows AS SELECT range AS i FROM range(%d)`, n))
	require.NoError(t, err)

	// No parameters: a parameter forces a rebind per execution and would hide a reused bind data.
	stmt := prepareCanceled(t, conn, `SELECT min(v) || ',' || max(v) || ',' || count(*) FROM (SELECT exec_ctx(i) AS v FROM exec_ctx_rows)`)
	for round := 1; round <= 3; round++ {
		calls.Store(0)
		want := fmt.Sprintf("exec-%d", round)
		got, err := execStatement(stmt, want, nil)
		require.NoError(t, err, "execution %d", round)
		require.Equal(t, []string{fmt.Sprintf("%s,%s,%d", want, want, n)}, got, "execution %d: every row must see the executing statement", round)
		require.Equal(t, int64(n), calls.Load(), "execution %d", round)
	}
}

func TestScalarUDFNotCalledAfterQueryReturns(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var calls atomic.Int64
	require.NoError(t, RegisterScalarUDF(conn, "exec_ctx", &execCtxUDF{
		config: execCtxBigintToVarchar(t),
		executor: ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
			calls.Add(1)
			return execCtxValue(ctx), nil
		}},
	}))

	// Several chunks. All calls must happen while the query call is still on the stack, i.e., while
	// the connection's context store holds the statement's context; fetching rows afterwards must not
	// run the function again.
	const n = 10_000
	// database/sql closes the rows when the query context is canceled, so it stays live until the end.
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), execCtxStmtKey{}, "exec"))
	defer cancel()
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT exec_ctx(i) FROM range(%d) t(i)`, n))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	require.Equal(t, int64(n), calls.Load(), "all calls happen before QueryContext returns")

	count := 0
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		require.Equal(t, "exec", v)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, n, count)
	require.Equal(t, int64(n), calls.Load(), "fetching rows does not call the function")
}

func TestScalarUDFContextInAppenderFlush(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	db := sql.OpenDB(c)
	conn := openConnWrapper(t, db, context.Background())

	varchar, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)
	var sawErr atomic.Value
	require.NoError(t, RegisterScalarUDF(conn, "exec_ctx_default", &execCtxUDF{
		config: ScalarFuncConfig{ResultTypeInfo: varchar, Volatile: true},
		executor: ScalarFuncExecutor{RowContextExecutor: func(ctx context.Context, _ []driver.Value) (any, error) {
			sawErr.Store(fmt.Sprint(ctx.Err()))
			return "default:" + execCtxValue(ctx), nil
		}},
	}))
	_, err = conn.ExecContext(context.Background(), `CREATE TABLE test (a BIGINT, b VARCHAR DEFAULT exec_ctx_default())`)
	require.NoError(t, err)

	// The appender runs DEFAULT expressions while flushing, outside any statement of this driver, so
	// the function sees a context without statement values that is never canceled.
	drvConn := openDriverConnWrapper(t, c)
	a, err := NewAppenderWithColumns(drvConn, "", "", "test", []string{"a"})
	require.NoError(t, err)
	require.NoError(t, a.AppendRow(int64(1)))
	require.NoError(t, a.Close())

	var b string
	require.NoError(t, conn.QueryRowContext(context.Background(), `SELECT b FROM test`).Scan(&b))
	require.Equal(t, "default:", b)
	require.Equal(t, "<nil>", sawErr.Load())

	closeDriverConnWrapper(t, &drvConn)
	closeConnWrapper(t, conn)
	closeDbWrapper(t, db)
	closeConnectorWrapper(t, c)
}
