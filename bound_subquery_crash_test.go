package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// crashUDF is a minimal scalar UDF with a ScalarBinder.
// The binder is the sole trigger: it causes DuckDB to invoke the bind callback,
// which calls duckdb_scalar_function_bind_get_argument for every argument.
// When an argument is a correlated subquery, that call internally invokes
// BoundSubqueryExpression::Copy(), which throws SerializationException.
// The C++ exception then unwinds through Go frames (which have no C++ unwind
// tables) and reaches std::terminate() — aborting the process.
type crashUDF struct{}

func (*crashUDF) Config() ScalarFuncConfig {
	v, _ := NewTypeInfo(TYPE_VARCHAR)
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{v, v},
		ResultTypeInfo: v,
	}
}

func (*crashUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		ScalarBinder: func(ctx context.Context, _ []ScalarUDFArg) (context.Context, error) {
			return ctx, nil
		},
		RowContextExecutor: func(ctx context.Context, vals []driver.Value) (any, error) {
			return vals[1], nil
		},
	}
}

const crashReproEnvVar = "DUCKDB_GO_RUN_CRASH_REPRO"

// TestBoundSubqueryCrashRepro reproduces the process abort caused by passing a
// correlated subquery as an argument to a ScalarBinder UDF.
//
// Crash chain:
//  1. DuckDB's query planner calls the bind callback during statement preparation.
//  2. The bind callback calls duckdb_scalar_function_bind_get_argument for each arg.
//  3. For a correlated subquery argument, DuckDB calls BoundSubqueryExpression::Copy().
//  4. Copy() throws SerializationException("Cannot copy BoundSubqueryExpression").
//  5. The C++ exception unwinds through Go frames, which have no C++ unwind tables,
//     so std::terminate() is called — SIGABRT, process dead.
//
// Two conditions must both be true to trigger the crash; remove either and it won't crash:
//   - The UDF has a ScalarBinder (triggers the bind callback)
//   - One argument is a correlated subquery (triggers Copy() inside the bind callback)
//
// Because the crash kills the process, the offending query runs in a subprocess
// (re-exec of this binary gated by an env var). The parent asserts the child exits
// non-zero and its output contains the expected DuckDB error message.
func TestBoundSubqueryCrashRepro(t *testing.T) {
	if os.Getenv(crashReproEnvVar) == "1" {
		runCrashReproQuery(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestBoundSubqueryCrashRepro$", "-test.v")
	cmd.Env = append(os.Environ(), crashReproEnvVar+"=1")
	out, err := cmd.CombinedOutput()

	if err == nil {
		t.Fatalf("expected subprocess to crash or fail, but it exited cleanly\nOutput:\n%s", out)
	}
	if !strings.Contains(string(out), "Cannot copy BoundSubqueryExpression") {
		t.Fatalf("subprocess exited non-zero but without the expected DuckDB message\nError: %v\nOutput:\n%s", err, out)
	}
	t.Logf("crash reproduced: subprocess exited with %q", "Cannot copy BoundSubqueryExpression")
}

func runCrashReproQuery(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := RegisterScalarUDF(conn, "f", &crashUDF{}); err != nil {
		t.Fatal(err)
	}

	// Conditions:
	//   1. f() has a ScalarBinder  → bind callback fires during planning
	//   2. second argument is a correlated subquery referencing outer column x
	//      → duckdb_scalar_function_bind_get_argument calls Copy() → throws
	rows, err := conn.QueryContext(ctx,
		`SELECT f('v', (SELECT f('v', x) FROM (VALUES(1)) _t)) FROM (VALUES('a')) tbl(x)`)
	if err != nil {
		// With the DuckDB fix the exception is caught and surfaced as an error here
		// instead of aborting. Embed the sentinel so the parent assertion still holds.
		t.Fatalf("Cannot copy BoundSubqueryExpression (returned as Go error): %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}
