package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPrepareQuery(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.Prepare(`SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err := prepared.Query(0)
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err = prepared.Query(0)
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_SELECT, stmtType)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, innerErr = stmt.ParamType(2)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		// Test column methods for SELECT * FROM foo
		columnCount, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 2, columnCount) // bar and baz columns

		// Test column names
		colName, innerErr := stmt.ColumnName(0)
		require.NoError(t, innerErr)
		require.Equal(t, "bar", colName)

		colName, innerErr = stmt.ColumnName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "baz", colName)

		// Test out of bounds - should return error
		colName, innerErr = stmt.ColumnName(2)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Empty(t, colName)

		// Test column types
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_VARCHAR, colType)

		colType, innerErr = stmt.ColumnType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, colType)

		// Test out of bounds - should return error
		colType, innerErr = stmt.ColumnType(2)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Equal(t, TYPE_INVALID, colType)

		r, innerErr := stmt.QueryBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.NoError(t, innerErr)

		// Don't immediately close the rows to trigger an active rows error.
		r, innerErr = stmt.QueryBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		badRows, innerErr := stmt.QueryBound(context.Background())
		require.ErrorIs(t, innerErr, errActiveRows)
		require.Nil(t, badRows)

		badResults, innerErr := stmt.ExecBound(context.Background())
		require.ErrorIs(t, innerErr, errActiveRows)
		require.Nil(t, badResults)

		require.NoError(t, r.Close())
		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		// Test column methods on closed statement
		_, innerErr = stmt.ColumnCount()
		require.ErrorIs(t, innerErr, errClosedStmt)

		_, innerErr = stmt.ColumnName(0)
		require.ErrorIs(t, innerErr, errClosedStmt)

		_, innerErr = stmt.ColumnTypeInfo(0)
		require.ErrorIs(t, innerErr, errClosedStmt)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareQueryPositional(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.Prepare(`SELECT $1, $2 AS foo WHERE foo = $2`)
	require.NoError(t, err)

	var foo, bar int
	row := prepared.QueryRow(1, 2)
	require.NoError(t, err)
	require.NoError(t, row.Scan(&foo, &bar))
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $2 AND baz = $1`)
	require.NoError(t, err)
	res, err := prepared.Query(0, "hello")
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Test column methods for positional SELECT
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT $1 AS first_param, $2 AS second_param`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// Test column count
		columnCount, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 1, columnCount)

		// Test column names
		colName, innerErr := stmt.ColumnName(0)
		require.NoError(t, innerErr)
		require.Equal(t, "unknown", colName)

		// Out of range - should return error
		colName, innerErr = stmt.ColumnName(1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Empty(t, colName)

		// Test column types - should be TYPE_INVALID for unresolved parameter types
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INVALID, colType)

		// Out of range - should return error
		colType, innerErr = stmt.ColumnType(1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Equal(t, TYPE_INVALID, colType)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `UPDATE foo SET bar = $2 WHERE baz = $1`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_UPDATE, stmtType)

		paramName, innerErr := stmt.ParamName(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Empty(t, paramName)

		paramName, innerErr = stmt.ParamName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "1", paramName)

		paramName, innerErr = stmt.ParamName(2)
		require.NoError(t, innerErr)
		require.Equal(t, "2", paramName)

		paramName, innerErr = stmt.ParamName(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Empty(t, paramName)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, innerErr = stmt.ParamType(2)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_VARCHAR, paramType)

		paramType, innerErr = stmt.ParamType(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		// Test column methods for UPDATE statement (should have a single Count column)
		columnCount, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 1, columnCount)

		colName, innerErr := stmt.ColumnName(0)
		require.NoError(t, innerErr)
		require.Equal(t, "Count", colName)

		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_BIGINT, colType)

		colTypeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_BIGINT, colTypeInfo.InternalType())

		r, innerErr := stmt.ExecBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.NoError(t, innerErr)

		r, innerErr = stmt.ExecBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, innerErr = stmt.ParamName(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Empty(t, paramName)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareQueryNamed(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.PrepareContext(context.Background(), `SELECT $foo, $bar, $baz, $foo`)
	require.NoError(t, err)

	var foo, bar, foo2 int
	var baz string
	row := prepared.QueryRow(sql.Named("baz", "x"), sql.Named("foo", 1), sql.Named("bar", 2))
	require.NoError(t, row.Scan(&foo, &bar, &baz, &foo2))
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	require.Equal(t, "x", baz)
	require.Equal(t, 1, foo2)

	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $bar AND baz = $baz`)
	require.NoError(t, err)
	res, err := prepared.Query(sql.Named("bar", "hello"), sql.Named("baz", 0))
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `INSERT INTO foo VALUES ($bar, $baz)`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_INSERT, stmtType)

		paramName, innerErr := stmt.ParamName(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Empty(t, paramName)

		paramName, innerErr = stmt.ParamName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "bar", paramName)

		paramName, innerErr = stmt.ParamName(2)
		require.NoError(t, innerErr)
		require.Equal(t, "baz", paramName)

		paramName, innerErr = stmt.ParamName(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Empty(t, paramName)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.Equal(t, TYPE_VARCHAR, paramType)
		require.NoError(t, innerErr)

		paramType, innerErr = stmt.ParamType(2)
		require.Equal(t, TYPE_INTEGER, paramType)
		require.NoError(t, innerErr)

		paramType, innerErr = stmt.ParamType(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		r, innerErr := stmt.ExecBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.NoError(t, innerErr)

		r, innerErr = stmt.ExecBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, innerErr = stmt.ParamName(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Empty(t, paramName)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		innerErr = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestUninitializedStmt(t *testing.T) {
	stmt := &Stmt{}

	stmtType, err := stmt.StatementType()
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

	paramType, err := stmt.ParamType(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, TYPE_INVALID, paramType)

	paramName, err := stmt.ParamName(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Empty(t, paramName)

	err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
	require.ErrorIs(t, err, errCouldNotBind)
	require.ErrorIs(t, err, errUninitializedStmt)

	// Test column methods on uninitialized statement
	_, err = stmt.ColumnCount()
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ColumnName(0)
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ColumnType(0)
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ColumnTypeInfo(0)
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ExecBound(context.Background())
	require.ErrorIs(t, err, errNotBound)
}

func TestPrepareWithError(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	testCases := []struct {
		sql string
		err string
	}{
		{
			sql: `SELECT * FROM tbl WHERE baz = ?`,
			err: `Table with name tbl does not exist`,
		},
		{
			sql: `SELECT * FROM foo WHERE col = ?`,
			err: `Referenced column "col" not found in FROM clause`,
		},
		{
			sql: `SELECT * FROM foo col = ?`,
			err: `syntax error at or near "="`,
		},
	}
	for _, tc := range testCases {
		prepared, err := db.Prepare(tc.sql)
		if err != nil {
			var dbErr *Error
			if !errors.As(err, &dbErr) {
				require.Fail(t, "error type is not (*duckdb.Error)")
			}
			require.ErrorContains(t, err, tc.err)
		}
		closePreparedWrapper(t, prepared)
	}
}

func TestPreparePivot(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE cities(country VARCHAR, name VARCHAR, year INT, population INT)`)
	_, err := db.ExecContext(ctx, `INSERT INTO cities VALUES ('NL', 'Netherlands', '2020', '42')`)
	require.NoError(t, err)

	prepared, err := db.Prepare(`PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	var country, name string
	var population int
	row := prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)

	prepared, err = db.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)
}

func TestBindWithoutResolvedParams(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	d := time.Date(2022, 0o2, 0o7, 0, 0, 0, 0, time.UTC)

	r := db.QueryRow(`SELECT a, b FROM (VALUES (?::TIMESTAMPTZ, ?::TIMESTAMPTZ)) t(a, b)`, d, d)
	var ta, tb time.Time
	require.NoError(t, r.Scan(&ta, &tb))
	require.True(t, ta.Equal(d))
	require.True(t, tb.Equal(d))

	s := []int32{1}
	r = db.QueryRow(`SELECT a::VARCHAR, b::VARCHAR FROM (VALUES (?, ?)) t(a, b)`, s, s)
	var sa, sb string
	require.NoError(t, r.Scan(&sa, &sb))
	require.Equal(t, "[1]", sa)
	require.Equal(t, "[1]", sb)

	// Type without a fallback.
	r = db.QueryRow(`SELECT a.strA FROM (VALUES (?),(?)) t(a)`, Union{Tag: "strA", Value: "a"}, Union{Tag: "strB", Value: "b"})
	require.Contains(t, r.Err().Error(), "unsupported type")
}

func TestBindTimestampTypes(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE OR REPLACE TABLE foo(a TIMESTAMP, b TIMESTAMP_S, c TIMESTAMP_MS, d TIMESTAMP_NS)`)
	require.NoError(t, err)

	tm := time.Date(2025, time.May, 7, 11, 45, 10, 123456789, time.UTC)
	_, err = db.Exec(`INSERT INTO foo VALUES(?, ?, ?, ?)`, tm, tm, tm, tm)
	require.NoError(t, err)

	r := db.QueryRow(`SELECT a, b, c, d FROM foo`)
	require.NoError(t, r.Err())

	var a, b, c, d time.Time
	err = r.Scan(&a, &b, &c, &d)
	require.NoError(t, err)

	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123456000, time.UTC), a)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 0, time.UTC), b)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123000000, time.UTC), c)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123456789, time.UTC), d)
}

func TestPrepareComplex(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE arr_test(
		arr_int INTEGER[2],
		list_str VARCHAR[],
		str_col STRUCT(v VARCHAR, i INTEGER)
	)`)

	// Insert parameters.
	_, err := db.ExecContext(ctx, `INSERT INTO arr_test VALUES (?, ?, ?)`,
		[]int32{7, 1}, []string{"foo", "bar"}, map[string]any{"v": "baz", "i": int32(42)})
	require.NoError(t, err)

	prepared, err := db.Prepare(`SELECT * FROM arr_test
		WHERE arr_int = ? AND list_str = ? AND str_col = ?`)
	defer closePreparedWrapper(t, prepared)
	require.NoError(t, err)

	var arr Composite[[]int32]
	var list Composite[[]string]
	var struc Composite[map[string]any]

	// Test with `any` slice types.
	err = prepared.QueryRow(
		[]any{int32(7), int32(1)},
		[]any{"foo", "bar"},
		map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())

	// Test with specific slice types.
	err = prepared.QueryRow(
		[]int32{7, 1},
		[]string{"foo", "bar"},
		map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())

	// Test querying without a prepared statement.
	err = db.QueryRow(`SELECT * FROM arr_test
		WHERE arr_int = ? AND list_str = ? AND str_col = ?`,
		[]int32{7, 1}, []string{"foo", "bar"}, map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())
}

func TestBindJSON(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE tbl (j JSON)`)
	require.NoError(t, err)

	jsonData := []byte(`{"name": "Jimmy","age": 28}`)
	_, err = db.Exec(`INSERT INTO tbl VALUES (?)`, jsonData)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tbl VALUES (?)`, nil)
	require.NoError(t, err)

	var str string
	err = db.QueryRow(`SELECT j::VARCHAR FROM tbl WHERE j IS NOT NULL`).Scan(&str)
	require.NoError(t, err)
	require.JSONEq(t, string(jsonData), str)

	var nilStr *string
	err = db.QueryRow(`SELECT j::VARCHAR FROM tbl WHERE j IS NULL`).Scan(&nilStr)
	require.NoError(t, err)
	require.Nil(t, nilStr)
}

func TestPrepareComplexQueryParameter(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE arr_test(
		arr_float float[2]
	)`)

	// Insert parameters.
	_, err := db.ExecContext(ctx, `INSERT INTO arr_test VALUES (?)`,
		[]float32{7, 1})
	require.NoError(t, err)

	prepared, err := db.Prepare(`SELECT list_value(?,?), list_cosine_distance(?,?)`)
	defer closePreparedWrapper(t, prepared)
	require.NoError(t, err)

	var arr Composite[[][]int32]
	var dis Composite[float32]
	// Test with specific slice types.
	err = prepared.QueryRow([]int{1, 2}, []int64{1}, []float32{0.1, 0.2, 0.3}, []float32{0.2, 0.3, 0.4}).Scan(&arr, &dis)
	require.NoError(t, err)
	require.Equal(t, [][]int32{{1, 2}, {1}}, arr.Get())
	require.Positive(t, dis.Get())

	dynamicPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, dynamicPrepare)
	require.NoError(t, err)

	var res Composite[[]any]
	err = dynamicPrepare.QueryRow([]any{}).Scan(&res)
	require.NoError(t, err)
	require.Empty(t, res.Get())

	// The statement type has already been bind as a SQL_NULL LIST in previous query
	err = dynamicPrepare.QueryRow([]any{1}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{nil}, res.Get())

	ptr := &([]int64{1})[0]
	ptrPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, ptrPrepare)
	require.NoError(t, err)

	err = ptrPrepare.QueryRow([]any{&([]int64{1})[0]}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, res.Get())

	nestedPtr := &ptr
	nestedPtrPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedPtrPrepare)
	require.NoError(t, err)

	err = nestedPtrPrepare.QueryRow([]any{nestedPtr}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, res.Get())

	arrayPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, arrayPrepare)
	require.NoError(t, err)

	err = arrayPrepare.QueryRow([1]any{123}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(123)}, res.Get())

	var nestedListRes Composite[[][]any]
	nestedListStmt, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedListStmt)
	require.NoError(t, err)

	err = nestedListStmt.QueryRow([][]float32{{0.1}, {0.2, 0.3}}).Scan(&nestedListRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{{float32(0.1)}, {float32(0.2), float32(0.3)}}, nestedListRes.Get())

	var nestedRes Composite[[][]any]
	nestedArrayPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, arrayPrepare)
	require.NoError(t, err)

	err = nestedArrayPrepare.QueryRow([2][2]float32{{0.1, 0.2}, {0.3, 0.4}}).Scan(&nestedRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{{float32(0.1), float32(0.2)}, {float32(0.3), float32(0.4)}}, nestedRes.Get())

	var tripleNestedRes Composite[[][][]string]
	tripleNestedPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, tripleNestedPrepare)
	require.NoError(t, err)

	err = tripleNestedPrepare.QueryRow([][][]string{{{"a"}, {"b", "c"}}, {{"1", "2"}, {"3"}}, {{"d", "e", "f"}}}).Scan(&tripleNestedRes)
	require.NoError(t, err)
	require.Equal(t, [][][]string{{{"a"}, {"b", "c"}}, {{"1", "2"}, {"3"}}, {{"d", "e", "f"}}}, tripleNestedRes.Get())

	var emptySliceRes Composite[[]any]
	emptySlicePrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, emptySlicePrepare)
	require.NoError(t, err)

	err = emptySlicePrepare.QueryRow([]float32{}).Scan(&emptySliceRes)
	require.NoError(t, err)
	require.Equal(t, []any{}, emptySliceRes.Get())

	var nestedEmptySliceRes Composite[[][]any]
	nestedEmptySlicePrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedEmptySlicePrepare)
	require.NoError(t, err)

	err = nestedEmptySlicePrepare.QueryRow([][]string{}).Scan(&nestedEmptySliceRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{}, nestedEmptySliceRes.Get())
}

func TestBindUUID(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Create table with nullable UUID column
	_, err := db.Exec(`CREATE TABLE uuid_test (id INTEGER, uuid_col UUID)`)
	require.NoError(t, err)

	// Test 1: Insert a NULL UUID using (*uuid.UUID)(nil)
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 1, (*uuid.UUID)(nil))
	require.NoError(t, err)

	// Test 2: Insert a NULL UUID using nil
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 2, nil)
	require.NoError(t, err)

	// Test 3: Insert a valid UUID ptr, to test complex value binding
	u3 := uuid.New()
	testUUID := UUID(u3)
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 3, &testUUID)
	require.NoError(t, err)

	// Test 4: Insert a uuid.UUID pointer containing a value, to test Stringer interface
	u4 := uuid.New()
	ptrToUUID := &u4
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 4, ptrToUUID)
	require.NoError(t, err)

	// Verify results by scanning back
	r, err := db.Query(`SELECT id, uuid_col FROM uuid_test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, r)

	expectedResults := []struct {
		id   int
		uuid *uuid.UUID
	}{
		{1, nil},
		{2, nil},
		{3, &u3},
		{4, &u4},
	}

	resultIndex := 0
	for r.Next() {
		var id int
		var retrievedUUID *uuid.UUID
		err = r.Scan(&id, &retrievedUUID)
		require.NoError(t, err)

		expected := expectedResults[resultIndex]
		require.Equal(t, expected.id, id, "incorrect id")

		if expected.uuid == nil {
			require.Nil(t, retrievedUUID)
		} else {
			require.NotNil(t, retrievedUUID)
			require.Equal(t, *expected.uuid, *retrievedUUID)
		}
		resultIndex++
	}
	require.Equal(t, 4, resultIndex, "incorrect count of results")

	// Verify NULL count
	var nullCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM uuid_test WHERE uuid_col IS NULL`).Scan(&nullCount)
	require.NoError(t, err)
	require.Equal(t, 2, nullCount, "incorrect count of NULLs")
}

// Define a custom type that shadows the string type
type String256 string

// Test that a custom type pointer is handled correctly by the DefaultParameterConverter
func TestInsertCustomTypePtr(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Create a table with a VARCHAR column
	createTable(t, db, `CREATE TABLE test_custom_type_ptr(id INTEGER, name VARCHAR)`)

	// Create a value of our custom type
	expected := String256("test string")

	// Insert the custom type pointer into the VARCHAR column
	// Ensures that the custom type is handled by the DefaultParameterConverter
	_, err := db.Exec(`INSERT INTO test_custom_type_ptr VALUES (?, ?)`, 1, &expected)
	require.NoError(t, err)

	// Query the inserted value back
	var id int
	var actual String256
	err = db.QueryRow(`SELECT id, name FROM test_custom_type_ptr WHERE id = ?`, 1).Scan(&id, &actual)
	require.NoError(t, err)

	// Verify the values
	require.Equal(t, 1, id, "incorrect id")
	require.Equal(t, expected, actual, "incorrect value")
}

func TestBindNullableValue(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Create a table with a nullable BIGINT/VARCHAR column
	createTable(t, db, `CREATE TABLE nullable_int_test(id BIGINT, name VARCHAR)`)

	// Test inserting a NULL value
	var nullID *int64
	var nullName *string
	_, err := db.Exec(`INSERT INTO nullable_int_test (id, name) VALUES (?, ?) RETURNING id, name`, nil, nil)
	require.NoError(t, err)

	// Query the inserted value back
	err = db.QueryRow(`SELECT id, name FROM nullable_int_test WHERE id IS NULL`).Scan(&nullID, &nullName)
	require.NoError(t, err)

	// Verify the values
	require.Nil(t, nullID, "expected id to be nil")
	require.Nil(t, nullName, "expected name to be nil")

	// Test inserting value pointer
	id := int64(42)
	name := "test name"
	var nonNullID *int64
	var nonNullName *string
	_, err = db.Exec(`INSERT INTO nullable_int_test (id, name) VALUES (?, ?) RETURNING id, name`, &id, &name)
	require.NoError(t, err)

	// Query the inserted value back
	err = db.QueryRow(`SELECT id, name FROM nullable_int_test WHERE id = ?`, id).Scan(&nonNullID, &nonNullName)
	require.NoError(t, err)

	// Verify the values
	require.NotNil(t, nonNullID, "expected id to not be nil")
	require.Equal(t, id, *nonNullID, "incorrect id value")
	require.NotNil(t, nonNullName, "expected name to not be nil")
	require.Equal(t, name, *nonNullName, "incorrect name value")
}

type testUUID string

func (u testUUID) String() string {
	return string(u)
}

type testUUIDList []testUUID

// Value implements driver.Valuer interface
func (l testUUIDList) Value() (driver.Value, error) {
	ss := make([]string, 0, len(l))
	for _, v := range l {
		ss = append(ss, v.String())
	}
	return fmt.Sprintf("[%s]", strings.Join(ss, ",")), nil
}

func TestDriverValuer(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE valuer_test (ids UUID [])`)

	list := testUUIDList{
		testUUID("123e4567-e89b-12d3-a456-426614174000"),
		testUUID("3a92e387-4b7d-4098-b273-967d48f6925f"),
	}
	_, err := db.Exec(`INSERT INTO valuer_test (ids) VALUES (?)`, list)
	require.NoError(t, err, "driver.Valuer should work for UUID arrays")

	_, err = db.Exec(`INSERT INTO valuer_test (ids) VALUES (?)`, "[123e4567-e89b-12d3-a456-426614174000,3a92e387-4b7d-4098-b273-967d48f6925f]")
	require.NoError(t, err, "string parameter should work for UUID arrays")

	_, err = db.Exec(`INSERT INTO valuer_test (ids) VALUES (?)`, any("[123e4567-e89b-12d3-a456-426614174000,3a92e387-4b7d-4098-b273-967d48f6925f]"))
	require.NoError(t, err, "any parameter should work for UUID arrays")

	// Expected to fail - no driver.Valuer implementation
	_, err = db.Exec(`INSERT INTO valuer_test (ids) VALUES (?)`, []uuid.UUID{uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), uuid.MustParse("3a92e387-4b7d-4098-b273-967d48f6925f")})
	require.Error(t, err, "[]uuid.UUID should fail without driver.Valuer")
	require.Contains(t, err.Error(), castErrMsg)
}

func TestMixedTypeSliceBinding(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE mixed_slice_test (foo TEXT)`)
	_, err := db.Exec(`INSERT INTO mixed_slice_test VALUES ('hello this is text'), ('other')`)
	require.NoError(t, err)

	sameTypeSlice := []any{"hello this is text", "other"}
	_, err = db.Query("FROM mixed_slice_test WHERE foo IN ?", sameTypeSlice)
	require.NoError(t, err)

	mixedSlice := []any{"hello this is text", 2}

	_, err = db.Query("FROM mixed_slice_test WHERE foo IN ?", mixedSlice)
	require.ErrorContains(t, err, "mixed types in slice: cannot bind VARCHAR (index 0) and BIGINT (index 1)")

	// Same test with named parameters
	_, err = db.Query("FROM mixed_slice_test WHERE foo IN $foo", sql.Named("foo", mixedSlice))
	require.ErrorContains(t, err, "mixed types in slice: cannot bind VARCHAR (index 0) and BIGINT (index 1)")

	nestedMixedSlice := [][]any{{"hello this is text"}, {2}}
	_, err = db.Query("FROM mixed_slice_test WHERE foo IN ?", nestedMixedSlice)
	require.ErrorContains(t, err, "mixed types in slice: cannot bind VARCHAR[] (index 0) and BIGINT[] (index 1)")
}

func TestInsertWithReturningClause(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE SEQUENCE location_id_seq START WITH 1 INCREMENT BY 1`)
	require.NoError(t, err)
	createTable(t, db, `CREATE TABLE location (
		id INTEGER PRIMARY KEY DEFAULT nextval('location_id_seq'),
		name TEXT NOT NULL
	)`)

	// INSERT without RETURNING using Exec
	res, err := db.Exec("INSERT INTO location (name) VALUES (?)", "test1")
	require.NoError(t, err)
	changes, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), changes)

	// INSERT with RETURNING using Exec - RowsAffected is 0 (known limitation)
	res, err = db.Exec("INSERT INTO location (name) VALUES (?) RETURNING id", "test2")
	require.NoError(t, err)
	changes, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), changes)

	// Verify both rows were inserted
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM location").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	// QueryRow
	var id int64
	err = db.QueryRow("INSERT INTO location (name) VALUES (?) RETURNING id", "test3").Scan(&id)
	require.NoError(t, err)
	require.Equal(t, int64(3), id)

	// Query
	rows, err := db.Query("INSERT INTO location (name) VALUES (?) RETURNING id", "test4")
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	require.True(t, rows.Next())
	err = rows.Scan(&id)
	require.NoError(t, err)
	require.Equal(t, int64(4), id)
	require.False(t, rows.Next())

	// Prepared statement
	stmt, err := db.Prepare("INSERT INTO location (name) VALUES (?) RETURNING id")
	require.NoError(t, err)
	defer closePreparedWrapper(t, stmt)
	err = stmt.QueryRow("test5").Scan(&id)
	require.NoError(t, err)
	require.Equal(t, int64(5), id)

	// Multiple RETURNING columns
	var name string
	err = db.QueryRow(
		"INSERT INTO location (name) VALUES (?) RETURNING id, name",
		"test6",
	).Scan(&id, &name)
	require.NoError(t, err)
	require.Equal(t, int64(6), id)
	require.Equal(t, "test6", name)

	// Verify final count
	err = db.QueryRow("SELECT COUNT(*) FROM location").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(6), count)
}

func TestRowsAffected(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE test_rows (id INTEGER, value TEXT)`)

	// Test INSERT - single row
	res, err := db.Exec("INSERT INTO test_rows VALUES (1, 'a')")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	// Test INSERT - multiple rows
	res, err = db.Exec("INSERT INTO test_rows VALUES (2, 'b'), (3, 'c'), (4, 'd')")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(3), affected)

	// Test UPDATE - single row
	res, err = db.Exec("UPDATE test_rows SET value = 'updated' WHERE id = 1")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	// Test UPDATE - multiple rows
	res, err = db.Exec("UPDATE test_rows SET value = 'batch' WHERE id IN (2, 3)")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), affected)

	// Test UPDATE - no matching rows
	res, err = db.Exec("UPDATE test_rows SET value = 'none' WHERE id = 999")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), affected)

	// Test DELETE - single row
	res, err = db.Exec("DELETE FROM test_rows WHERE id = 4")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	// Test DELETE - multiple rows
	res, err = db.Exec("DELETE FROM test_rows WHERE id IN (2, 3)")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), affected)

	// Test DELETE - no matching rows
	res, err = db.Exec("DELETE FROM test_rows WHERE id = 999")
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), affected)

	// Verify only row with id=1 remains
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM test_rows").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestInterrupt(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	// Long-running query.
	go func() {
		_, err := db.ExecContext(ctx, "CREATE TABLE t AS SELECT range::VARCHAR, random() AS k FROM range(1_000_000_000) ORDER BY k")
		done <- err
	}()

	// Interrupt it.
	time.Sleep(1 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.ErrorContains(t, err, "INTERRUPT Error: Interrupted!")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "TestInterrupt timed out waiting for query to finish after cancel")
	}
}

func TestPreparedStatementColumnMethods(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE test_columns (id INTEGER, name VARCHAR, value DOUBLE, created_at TIMESTAMP)`)

	// Prepare a SELECT statement
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	err := conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT id, name, value, created_at FROM test_columns`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// Test ColumnCount
		count, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 4, count)

		// Test ColumnName
		name, innerErr := stmt.ColumnName(0)
		require.NoError(t, innerErr)
		require.Equal(t, "id", name)

		name, innerErr = stmt.ColumnName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "name", name)

		name, innerErr = stmt.ColumnName(2)
		require.NoError(t, innerErr)
		require.Equal(t, "value", name)

		name, innerErr = stmt.ColumnName(3)
		require.NoError(t, innerErr)
		require.Equal(t, "created_at", name)

		// Test out of bounds - should return error
		name, innerErr = stmt.ColumnName(-1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Empty(t, name)

		name, innerErr = stmt.ColumnName(4)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Empty(t, name)

		// Test ColumnType
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, colType)

		colType, innerErr = stmt.ColumnType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_VARCHAR, colType)

		colType, innerErr = stmt.ColumnType(2)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_DOUBLE, colType)

		colType, innerErr = stmt.ColumnType(3)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_TIMESTAMP, colType)

		// Test out of bounds - should return error
		colType, innerErr = stmt.ColumnType(-1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Equal(t, TYPE_INVALID, colType)

		colType, innerErr = stmt.ColumnType(4)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Equal(t, TYPE_INVALID, colType)

		// Test ColumnTypeInfo - should return TypeInfo for each column
		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_INTEGER, typeInfo.InternalType())

		typeInfo, innerErr = stmt.ColumnTypeInfo(1)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_VARCHAR, typeInfo.InternalType())

		typeInfo, innerErr = stmt.ColumnTypeInfo(2)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_DOUBLE, typeInfo.InternalType())

		typeInfo, innerErr = stmt.ColumnTypeInfo(3)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_TIMESTAMP, typeInfo.InternalType())

		// Test out of bounds - should return error
		typeInfo, innerErr = stmt.ColumnTypeInfo(4)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Nil(t, typeInfo)

		typeInfo, innerErr = stmt.ColumnTypeInfo(-1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Nil(t, typeInfo)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)
}

func TestPreparedStatementColumnTypeInfo(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	// Test with complex types
	err := conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		// Create a query with ARRAY, LIST, and STRUCT types
		s, innerErr := innerConn.PrepareContext(context.Background(),
			`SELECT [1, 2, 3]::INTEGER[3] AS arr_col,
			        [4, 5, 6] AS list_col,
			        {'x': 10, 'y': 20} AS struct_col`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// Test ARRAY column
		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_ARRAY, typeInfo.InternalType())

		// Assert ARRAY details
		details := typeInfo.Details()
		require.NotNil(t, details)
		arrayDetails, ok := details.(*ArrayDetails)
		require.True(t, ok, "Expected ArrayDetails")
		require.Equal(t, TYPE_INTEGER, arrayDetails.Child.InternalType())
		require.Equal(t, uint64(3), arrayDetails.Size)

		// Test LIST column
		typeInfo, innerErr = stmt.ColumnTypeInfo(1)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_LIST, typeInfo.InternalType())

		// Assert LIST details
		details = typeInfo.Details()
		require.NotNil(t, details)
		listDetails, ok := details.(*ListDetails)
		require.True(t, ok, "Expected ListDetails")
		require.Equal(t, TYPE_INTEGER, listDetails.Child.InternalType())

		// Test STRUCT column
		typeInfo, innerErr = stmt.ColumnTypeInfo(2)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_STRUCT, typeInfo.InternalType())

		// Assert STRUCT details
		details = typeInfo.Details()
		require.NotNil(t, details)
		structDetails, ok := details.(*StructDetails)
		require.True(t, ok, "Expected StructDetails")
		require.Len(t, structDetails.Entries, 2)

		// Check first field 'x'
		require.Equal(t, "x", structDetails.Entries[0].Name())
		require.Equal(t, TYPE_INTEGER, structDetails.Entries[0].Info().InternalType())

		// Check second field 'y'
		require.Equal(t, "y", structDetails.Entries[1].Name())
		require.Equal(t, TYPE_INTEGER, structDetails.Entries[1].Info().InternalType())

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test with DECIMAL type
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT 123.45::DECIMAL(10,2) AS dec_col`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_DECIMAL, typeInfo.InternalType())

		// Assert DECIMAL details
		details := typeInfo.Details()
		require.NotNil(t, details)
		decimalDetails, ok := details.(*DecimalDetails)
		require.True(t, ok, "Expected DecimalDetails")
		require.Equal(t, uint8(10), decimalDetails.Width)
		require.Equal(t, uint8(2), decimalDetails.Scale)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test with ENUM type
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		// Create an ENUM type first
		_, innerErr := innerConn.ExecContext(context.Background(),
			`CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')`, nil)
		require.NoError(t, innerErr)

		s, innerErr := innerConn.PrepareContext(context.Background(),
			`SELECT 'happy'::mood AS mood_col`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_ENUM, typeInfo.InternalType())

		// Assert ENUM details
		details := typeInfo.Details()
		require.NotNil(t, details)
		enumDetails, ok := details.(*EnumDetails)
		require.True(t, ok, "Expected EnumDetails")
		require.Equal(t, []string{"happy", "sad", "neutral"}, enumDetails.Values)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test with MAP type
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		s, innerErr := innerConn.PrepareContext(context.Background(),
			`SELECT MAP([1, 2], ['a', 'b']) AS map_col`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_MAP, typeInfo.InternalType())

		// Assert MAP details
		details := typeInfo.Details()
		require.NotNil(t, details)
		mapDetails, ok := details.(*MapDetails)
		require.True(t, ok, "Expected MapDetails")
		require.Equal(t, TYPE_INTEGER, mapDetails.Key.InternalType())
		require.Equal(t, TYPE_VARCHAR, mapDetails.Value.InternalType())

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test with nested types: LIST of STRUCTs
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		s, innerErr := innerConn.PrepareContext(context.Background(),
			`SELECT [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}] AS list_struct_col`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		typeInfo, innerErr := stmt.ColumnTypeInfo(0)
		require.NoError(t, innerErr)
		require.NotNil(t, typeInfo)
		require.Equal(t, TYPE_LIST, typeInfo.InternalType())

		// Assert LIST details
		details := typeInfo.Details()
		require.NotNil(t, details)
		listDetails, ok := details.(*ListDetails)
		require.True(t, ok, "Expected ListDetails")
		require.Equal(t, TYPE_STRUCT, listDetails.Child.InternalType())

		// Assert nested STRUCT details
		structDetails, ok := listDetails.Child.Details().(*StructDetails)
		require.True(t, ok, "Expected StructDetails for nested type")
		require.Len(t, structDetails.Entries, 2)
		require.Equal(t, "id", structDetails.Entries[0].Name())
		require.Equal(t, TYPE_INTEGER, structDetails.Entries[0].Info().InternalType())
		require.Equal(t, "name", structDetails.Entries[1].Name())
		require.Equal(t, TYPE_VARCHAR, structDetails.Entries[1].Info().InternalType())

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)
}

func TestPreparedStatementAmbiguousColumnTypes(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	// Test cases where column types cannot be resolved
	err := conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		// Test 1: VALUES clause without type casting - ambiguous types
		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT * FROM (VALUES (?, ?)) t(a, b)`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// When columns have ambiguous types, count becomes 1
		count, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 1, count)

		// Column type should be INVALID
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INVALID, colType)

		// Out of bounds access - should return error
		colType, innerErr = stmt.ColumnType(1)
		require.Error(t, innerErr)
		require.ErrorIs(t, innerErr, errAPI)
		require.Equal(t, TYPE_INVALID, colType)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test 2: Direct parameter selection - all ambiguous
	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT ?, ?, ? + ?`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// When columns have ambiguous types, count becomes 1
		count, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 1, count)

		// Column type should be INVALID
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INVALID, colType)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	// Test 3: Mixed known and unknown types
	createTable(t, db, `CREATE TABLE test_mixed (id INTEGER, value VARCHAR)`)

	err = conn.Raw(func(driverConn any) error {
		innerConn := driverConn.(*Conn)

		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT id, value, ? AS param_col FROM test_mixed`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		// When any column has ambiguous type, count becomes 1
		count, innerErr := stmt.ColumnCount()
		require.NoError(t, innerErr)
		require.Equal(t, 1, count)

		// All column types become INVALID
		colType, innerErr := stmt.ColumnType(0)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INVALID, colType)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)
}

func TestTimestampBinding(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	startTime := time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC)
	endTime := time.Date(2026, 1, 8, 13, 0, 0, 0, time.UTC)

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	startTimeNY := time.Date(2026, 1, 8, 7, 0, 0, 0, loc)
	endTimeNY := time.Date(2026, 1, 8, 8, 0, 0, 0, loc)

	// Interpolated query returns 13 rows
	interpolatedQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM generate_series(
			time_bucket(INTERVAL '5 minutes', '%s'::timestamptz),
			'%s'::timestamptz,
			INTERVAL '5 minutes'
		) AS t(epoch_ts)
	`, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

	var expectedCount int

	err = db.QueryRow(interpolatedQuery).Scan(&expectedCount)
	require.NoError(t, err)
	require.Equal(t, 13, expectedCount)

	// Bound query returns 73 rows with parameters without the fix.
	boundedQuery := `
		SELECT COUNT(*) FROM generate_series(
			time_bucket(INTERVAL '5 minutes', ?::timestamptz),
			?::timestamptz,
			INTERVAL '5 minutes'
		) AS t(epoch_ts)
	`

	err = db.QueryRow(boundedQuery, startTime, endTime).Scan(&expectedCount)
	require.NoError(t, err)
	require.Equal(t, 13, expectedCount)

	err = db.QueryRow(boundedQuery, startTimeNY, endTimeNY).Scan(&expectedCount)
	require.NoError(t, err)
	require.Equal(t, 13, expectedCount)
}
