package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"os/exec"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

type crashUDF struct{}

func (*crashUDF) Config() duckdb.ScalarFuncConfig {
	v, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{v, v},
		ResultTypeInfo: v,
	}
}

func (*crashUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		ScalarBinder: func(ctx context.Context, _ []duckdb.ScalarUDFArg) (context.Context, error) {
			return ctx, nil
		},
		RowContextExecutor: func(ctx context.Context, vals []driver.Value) (any, error) {
			return vals[1], nil
		},
	}
}

const envVar = "DUCKDB_GO_RUN_CRASH_REPRO"

func main() {
	if os.Getenv(envVar) == "1" {
		runQuery()
		return
	}

	fmt.Println("Spawning subprocess to run the offending query...")

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), envVar+"=1")
	out, err := cmd.CombinedOutput()

	if err == nil {
		fmt.Printf("UNEXPECTED: subprocess exited cleanly — crash not reproduced.\nOutput:\n%s\n", out)
		os.Exit(1)
	}
	if strings.Contains(string(out), "Cannot copy BoundSubqueryExpression") {
		fmt.Printf("REPRODUCED: subprocess exited with the expected DuckDB error.\nOutput:\n%s\n", out)
	} else {
		fmt.Printf("UNEXPECTED: subprocess crashed but with a different error.\nError: %v\nOutput:\n%s\n", err, out)
		os.Exit(1)
	}
}

func runQuery() {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "conn:", err)
		os.Exit(1)
	}
	defer conn.Close()

	if err := duckdb.RegisterScalarUDF(conn, "f", &crashUDF{}); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		os.Exit(1)
	}

	rows, err := conn.QueryContext(ctx,
		`SELECT f('v', (SELECT f('v', x) FROM (VALUES(1)) _t)) FROM (VALUES('a')) tbl(x)`)
	if err != nil {
		// With the DuckDB fix the exception is caught and returned as a Go error.
		fmt.Fprintf(os.Stderr, "Cannot copy BoundSubqueryExpression (returned as Go error): %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
	}
}
