package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testErrorInternal(t *testing.T, actual error, contains []string) {
	for _, msg := range contains {
		require.Contains(t, actual.Error(), msg)
	}

	levels := strings.Count(actual.Error(), driverErrMsg)
	require.Equal(t, 1, levels)
}

func testError(t *testing.T, actual error, contains ...string) {
	testErrorInternal(t, actual, contains)
}

func TestGetErrorWrapsCause(t *testing.T) {
	cause := errors.New("wrapped cause")

	err := getError(errAppenderFlush, cause)
	require.ErrorIs(t, err, errAppenderFlush)
	require.ErrorIs(t, err, cause)
}

func TestErrConnect(t *testing.T) {
	t.Run(errParseDSN.Error(), func(t *testing.T) {
		db, err := sql.Open(`duckdb`, `:mem ory:`)
		defer closeDbWrapper(t, db)
		testError(t, err, errParseDSN.Error())
	})

	t.Run(errConnect.Error(), func(t *testing.T) {
		db, err := sql.Open(`duckdb`, `?readonly`)
		defer closeDbWrapper(t, db)
		testError(t, err, errConnect.Error())
	})

	t.Run(errSetConfig.Error(), func(t *testing.T) {
		db, err := sql.Open(`duckdb`, `?threads=NaN`)
		defer closeDbWrapper(t, db)
		testError(t, err, errSetConfig.Error())
	})

	t.Run("local config option", func(t *testing.T) {
		db, err := sql.Open(`duckdb`, `?schema=main`)
		defer closeDbWrapper(t, db)
		testError(t, err, errSetConfig.Error())
	})
}

func TestErrNestedMap(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var m Map
	err := db.QueryRow(`SELECT MAP([MAP([1], [1]), MAP([2], [2])], ['a', 'e'])`).Scan(&m)
	testError(t, err, errUnsupportedMapKeyType.Error())
}

func TestErrUncomparableMapKey(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Every query holds two entries. The panic needs a second key to compare against, so a
	// single-entry row cannot reach the failing state and would pass on its own. The one
	// deliberate exception is marked below, and it asserts an error rather than a scan.
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// These scan to []byte, or to a struct wrapping one.
		{
			name:    "BLOB key",
			query:   `SELECT MAP{'\x01'::BLOB: 'x', '\x02'::BLOB: 'y'}`,
			wantErr: true,
		},
		// The exception. A single-entry BLOB map used to scan successfully, because Delete
		// compares nothing while the map is still empty. It is rejected now, and that size
		// independence is a behaviour change worth pinning: a guard that only ran on the
		// second key would let this row scan again with nothing else failing.
		{
			name:    "BLOB key, single entry",
			query:   `SELECT MAP{'\x01'::BLOB: 'x'}`,
			wantErr: true,
		},
		{
			name:    "BIT key",
			query:   `SELECT MAP{'101'::BIT: 'x', '110'::BIT: 'y'}`,
			wantErr: true,
		},
		{
			name:    "GEOMETRY key",
			query:   `SELECT MAP{'POINT(1 2)'::GEOMETRY: 'x', 'POINT(3 4)'::GEOMETRY: 'y'}`,
			wantErr: true,
		},
		// UUID keys scan to []byte, because hugeIntToUUID returns val[:]. The type ID says
		// TYPE_UUID, so only a check on the scanned value catches this one.
		{
			name:    "UUID key",
			query:   `SELECT MAP{'80000000-0000-0000-0000-000000000000'::UUID: 'x', '80000000-0000-0000-0000-000000000001'::UUID: 'y'}`,
			wantErr: true,
		},
		// A JSON alias reports TYPE_VARCHAR, but a JSON object scans to map[string]any.
		{
			name:    "JSON object key",
			query:   `SELECT MAP(['{"a":1}'::JSON, '{"b":2}'::JSON], ['x', 'y'])`,
			wantErr: true,
		},
		{
			name:    "LIST key",
			query:   `SELECT MAP{[1]: 'x', [2]: 'y'}`,
			wantErr: true,
		},
		// UNION is rejected by its type ID in initMap, and it has to be: getUnion returns a
		// struct with a driver.Value field, which reflect reports as comparable even though
		// == panics once that field holds a []byte. Dropping TYPE_UNION from initMap brings
		// the panic back for the BLOB member, so both members are pinned here.
		{
			name:    "UNION key, uncomparable member",
			query:   `SELECT MAP([union_value(b := 'abc'::BLOB), union_value(b := 'def'::BLOB)], ['x', 'y'])`,
			wantErr: true,
		},
		{
			name:    "UNION key, comparable member",
			query:   `SELECT MAP([union_value(i := 1), union_value(i := 2)], ['x', 'y'])`,
			wantErr: true,
		},
		// Comparable key types must keep working; the guard must not over-reject.
		{
			name:  "VARCHAR key",
			query: `SELECT MAP{'a': 'x', 'b': 'y'}`,
		},
		{
			name:  "INTEGER key",
			query: `SELECT MAP{1: 'x', 2: 'y'}`,
		},
		// The same alias as the rejected row above, holding scalars this time. It proves the
		// guard follows the scanned value rather than the alias.
		{
			name:  "JSON scalar key",
			query: `SELECT MAP(['1'::JSON, '2'::JSON], ['x', 'y'])`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var m OrderedMap
			err := db.QueryRow(test.query).Scan(&m)

			if test.wantErr {
				// testError dereferences the error, so assert it is there first. Without
				// this, a row that starts scanning again reports a nil dereference inside
				// the helper rather than the expectation it broke.
				require.Error(t, err)
				testError(t, err, errUnsupportedMapKeyType.Error())
				// The column index is added once, by whoever called into the vector.
				// A guard that adds it again reports "index: 0: index: 0".
				require.Equal(t, 1, strings.Count(err.Error(), indexErrMsg))
				return
			}
			require.NoError(t, err)
			// Both keys must survive. A collapsed length would mean the keys compared equal
			// when they are not.
			require.Equal(t, 2, m.Len())
		})
	}
}

func TestErrAppender(t *testing.T) {
	t.Run(errInvalidCon.Error(), func(t *testing.T) {
		var conn driver.Conn
		a, err := NewAppenderFromConn(conn, "", "test")
		defer closeAppenderWrapper(t, a)
		testError(t, err, errInvalidCon.Error())
	})

	t.Run(errClosedCon.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		closeDriverConnWrapper(t, &conn)

		a, err := NewAppenderFromConn(conn, "", "test")
		defer closeAppenderWrapper(t, a)
		testError(t, err, errClosedCon.Error())
	})

	t.Run(errAppenderCreation.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		a, err := NewAppenderFromConn(conn, "", "does_not_exist")
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderCreation.Error())
	})

	t.Run(errAppenderCreation.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		appendQuery := `INSERT INTO test FROM appended_data`

		a, err := NewTableAppender(conn, appendQuery, "", "", "does_not_exist", []string{})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderCreation.Error(), "No table with that schema+name could be located")

		// Create the table.
		db := sql.OpenDB(c)
		_, err = db.Exec(`CREATE TABLE test (i INT)`)
		require.NoError(t, err)

		a, err = NewTableAppender(conn, appendQuery, "", "", "test", []string{"a", "a"})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderDuplicateColumn.Error())

		a, err = NewTableAppender(conn, appendQuery, "", "", "test", []string{"i", "b"})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderColumnMismatch.Error())
	})

	t.Run(errAppenderEmptyQuery.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		a, err := NewQueryAppender(conn, "", "", []TypeInfo{}, []string{})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderEmptyQuery.Error())
	})

	t.Run(errAppenderEmptyColumnTypes.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		a, err := NewQueryAppender(conn, `INSERT INTO test SELECT * FROM appended_data`, "", []TypeInfo{}, []string{"c1", "c2"})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderEmptyColumnTypes.Error())
	})

	t.Run(errAppenderColumnMismatch.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		info, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		a, err := NewQueryAppender(conn, `INSERT INTO test SELECT * FROM appended_data`, "", []TypeInfo{info}, []string{"c1", "c2"})
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderColumnMismatch.Error())
	})

	t.Run(errAppenderDoubleClose.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		db := sql.OpenDB(c)
		defer closeDbWrapper(t, db)
		_, err := db.Exec(`CREATE TABLE tbl (i INTEGER)`)
		require.NoError(t, err)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		a, err := NewAppenderFromConn(conn, "", "tbl")
		closeAppenderWrapper(t, a)
		require.NoError(t, err)

		err = a.Close()
		testError(t, err, errAppenderDoubleClose.Error())
	})

	t.Run(columnCountErrMsg, func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (a VARCHAR, b VARCHAR)`)
		defer cleanupAppender(t, c, db, conn, a)
		err := a.AppendRow("hello")
		testError(t, err, errAppenderAppendRow.Error(), columnCountErrMsg)
	})

	t.Run(errAppenderAppendAfterClose.Error(), func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (str VARCHAR)`)
		closeAppenderWrapper(t, a)
		defer closeDriverConnWrapper(t, &conn)
		defer closeDbWrapper(t, db)
		defer closeConnectorWrapper(t, c)

		err := a.AppendRow("hello")
		testError(t, err, errAppenderAppendAfterClose.Error())
	})

	t.Run(errAppenderFlush.Error(), func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (c1 INTEGER PRIMARY KEY)`)
		defer closeDriverConnWrapper(t, &conn)
		defer closeDbWrapper(t, db)
		defer closeConnectorWrapper(t, c)

		require.NoError(t, a.AppendRow(int32(1)))
		require.NoError(t, a.AppendRow(int32(1)))
		err := a.Flush()
		testError(t, err, errAppenderFlush.Error())

		err = a.Close()
		testError(t, err, errAppenderClose.Error())
	})

	t.Run(errAppenderClose.Error(), func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (c1 INTEGER PRIMARY KEY)`)
		defer closeDriverConnWrapper(t, &conn)
		defer closeDbWrapper(t, db)
		defer closeConnectorWrapper(t, c)

		require.NoError(t, a.AppendRow(int32(1)))
		require.NoError(t, a.AppendRow(int32(1)))

		err := a.Close()
		testError(t, err, errAppenderClose.Error())
	})

	t.Run(errUnsupportedMapKeyType.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, ``, nil)
		defer closeConnectorWrapper(t, c)

		db := sql.OpenDB(c)
		_, err := db.Exec(`CREATE TABLE test (m MAP(INT[], STRUCT(v INT)))`)
		require.NoError(t, err)
		defer closeDbWrapper(t, db)

		conn := openDriverConnWrapper(t, c)
		defer closeDriverConnWrapper(t, &conn)

		a, err := NewAppenderFromConn(conn, "", "test")
		defer closeAppenderWrapper(t, a)
		testError(t, err, errAppenderCreation.Error(), errUnsupportedMapKeyType.Error())
	})

	t.Run(invalidInputErrMsg, func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (col INT[3])`)
		defer cleanupAppender(t, c, db, conn, a)
		err := a.AppendRow([]int32{1, 2})
		testError(t, err, errAppenderAppendRow.Error(), invalidInputErrMsg)
	})

	t.Run(setValueErrMsg, func(t *testing.T) {
		c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (col float)`)
		defer cleanupAppender(t, c, db, conn, a)
		err := a.AppendRow("test")
		testError(t, err, errAppenderAppendRow.Error(), setValueErrMsg)
	})
}

func TestErrAppend(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (id BIGINT, str VARCHAR)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow("hello", "world")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(false, 42)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendDecimal(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (d DECIMAL(8, 2))`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow(Decimal{Width: 9, Scale: 2})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(Decimal{Width: 8, Scale: 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendEnum(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, testTypesEnumSQL+";"+`CREATE TABLE test (e my_enum)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow("3")
	testError(
		t,
		err,
		errAppenderAppendRow.Error(),
		invalidInputErrMsg,
		"expected value in enum dictionary, got 3",
	)
}

func TestErrAppendSimpleStruct(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `
		CREATE TABLE test (
			simple_struct STRUCT(A INT, B VARCHAR)
		)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow(1)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow("hello")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	type other struct {
		S string
		I int
	}
	err = a.AppendRow(other{"hello", 1})
	testError(t, err, errAppenderAppendRow.Error(), structFieldErrMsg)

	err = a.AppendRow(
		wrappedSimpleStruct{
			"hello there",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	err = a.AppendRow(
		wrappedStruct{
			"hello there",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	testError(t, err, errAppenderAppendRow.Error(), structFieldErrMsg)
}

func TestErrAppendDuplicateStruct(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `
		CREATE TABLE test (
			duplicate_struct STRUCT(Duplicate INT)
		)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow(duplicateKeyStruct{1, 2})
	testError(t, err, errAppenderAppendRow.Error(), duplicateNameErrMsg)
}

func TestErrAppendStruct(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `
		CREATE TABLE test (
			mix STRUCT(a STRUCT(L VARCHAR[]), B STRUCT(L INT[])[])
		)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendList(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test(intSlice INT[])`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow([]string{"foo", "bar", "baz"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendStructWithList(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (struct_with_list STRUCT(L INT[]))`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	l := struct{ L []string }{L: []string{"a", "b", "c"}}
	testError(t, a.AppendRow(l), errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendNestedStruct(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `
		CREATE TABLE test (
			wrapped_simple_struct STRUCT(a VARCHAR, B STRUCT(A INT, B VARCHAR)),
		)`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendNestedList(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test(int_slice INT[][][])`)
	defer cleanupAppender(t, c, db, conn, a)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(1)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppenderTSConversion(t *testing.T) {
	testCases := []string{"TIMESTAMP_NS", "TIMESTAMP", "TIMESTAMPTZ"}
	for _, tc := range testCases {
		t.Run(tc+" conversion error", func(t *testing.T) {
			c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (t `+tc+`)`)
			defer cleanupAppender(t, c, db, conn, a)

			tsLess := time.Date(-290407, time.January, 1, 15, 0o4, 5, 123456, time.UTC)
			err := a.AppendRow(tsLess)
			testError(t, err, errAppenderAppendRow.Error(), convertErrMsg)

			tsGreater := time.Date(294346, time.January, 1, 15, 0o4, 5, 123456, time.UTC)
			err = a.AppendRow(tsGreater)
			testError(t, err, errAppenderAppendRow.Error(), convertErrMsg)
		})
	}
}

func TestErrAPISetValue(t *testing.T) {
	var chunk DataChunk
	err := chunk.SetValue(1, 42, "hello")
	testError(t, err, errAPI.Error(), columnCountErrMsg)
	err = SetChunkValue(chunk, 1, 42, "hello")
	testError(t, err, errAPI.Error(), columnCountErrMsg)
}

func TestDuckDBErrors(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE duckdb_error_test(bar VARCHAR UNIQUE, baz INT32, u_1 UNION("string" VARCHAR))`)
	_, err := db.Exec(`INSERT INTO duckdb_error_test(bar, baz) VALUES ('bar', 0)`)
	require.NoError(t, err)

	testCases := []struct {
		tpl    string
		errTyp ErrorType
	}{
		{
			tpl:    `SELECT * FROM not_exist WHERE baz=0`,
			errTyp: ErrorTypeCatalog,
		},
		{
			tpl:    `SELECT * FROM duckdb_error_test WHERE col=?`,
			errTyp: ErrorTypeBinder,
		},
		{
			tpl:    `SELEC * FROM duckdb_error_test baz=0`,
			errTyp: ErrorTypeParser,
		},
		{
			tpl:    `INSERT INTO duckdb_error_test(bar, baz) VALUES ('bar', 1)`,
			errTyp: ErrorTypeConstraint,
		},
		{
			tpl:    `INSERT INTO duckdb_error_test(bar, baz) VALUES ('foo', 18446744073709551615)`,
			errTyp: ErrorTypeConversion,
		},
		{
			tpl:    `LOAD not_exist`,
			errTyp: ErrorTypeIO,
		},
		{
			tpl:    `SELECT array_length(array_value(array_value(1, 2, 2), array_value(3, 4, 3)), 3)`,
			errTyp: ErrorTypeOutOfRange,
		},
		{
			tpl:    `SELECT '010110'::BIT & '11000'::BIT`,
			errTyp: ErrorTypeInvalidInput,
		},
		{
			tpl:    `SET external_threads=-1`,
			errTyp: ErrorTypeInvalidInput,
		},
		{
			tpl:    `CREATE UNIQUE INDEX idx ON duckdb_error_test(u_1)`,
			errTyp: ErrorTypeInvalidType,
		},
	}
	for _, tc := range testCases {
		_, err = db.Exec(tc.tpl)
		var de *Error
		ok := errors.As(err, &de)
		if !ok {
			require.Fail(t, "error type is not (*duckdb.Error)", "tql: %s\ngot: %#v", tc.tpl, err)
		}
		require.Equal(t, de.Type, tc.errTyp, "tpl: %s\nactual error msg: %s", tc.tpl, de.Msg)
	}
}

func TestDuckDBErrorsCornerCases(t *testing.T) {
	testCases := []*Error{
		{
			Msg:  "",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Unknown",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Error: xxx",
			Type: ErrorTypeUnknownType,
		},
		// Prefix testing.
		{
			Msg:  "Invalid Error: xxx",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Invalid Input Error: xxx",
			Type: ErrorTypeInvalidInput,
		},
		{
			Msg:  "Invalid Configuration Error: xxx",
			Type: ErrorTypeInvalidConfiguration,
		},
	}

	for _, tc := range testCases {
		var err *Error
		errors.As(getDuckDBError(tc.Msg), &err)
		require.Equal(t, tc, err)
	}
}

type wrappedDuckDBError struct {
	e *Error
}

func (w *wrappedDuckDBError) Error() string {
	return w.e.Error()
}

func (w *wrappedDuckDBError) Unwrap() error {
	return w.e
}

func TestGetDuckDBErrorIs(t *testing.T) {
	const errMsg = "Out of Range Error: Overflow"
	outOfRangeErr1 := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  errMsg,
	}
	outOfRangeErr1Copy := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  errMsg,
	}
	outOfRangeErr2 := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  "Out of Range Error: array_length dimension '3' out of range (min: '1', max: '2')",
	}
	invalidInputErr := &Error{
		Type: ErrorTypeInvalidInput,
		Msg:  "Invalid Input Error: Map keys can not be NULL",
	}

	require.ErrorIs(t, outOfRangeErr1Copy, outOfRangeErr1)
	require.ErrorIs(t, &wrappedDuckDBError{outOfRangeErr1Copy}, outOfRangeErr1)
	require.NotErrorIs(t, outOfRangeErr2, outOfRangeErr1)
	require.NotErrorIs(t, invalidInputErr, outOfRangeErr1)
	require.NotErrorIs(t, errors.New(errMsg), outOfRangeErr1)
}
