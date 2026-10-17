package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A LIST parameter whose Go elements are not the Go type of the declared element type, e.g. []int64 bound to
// `unnest(?::INTEGER[])`, used to panic in createPrimitiveValue's type assertion. The values are now bound as they are
// and DuckDB casts them to the parameter type.

func unnestCastQuery(target string) string {
	return "SELECT coalesce(string_agg(v::VARCHAR, ',' ORDER BY ord), '') || '|' || count(*)" +
		" FROM (SELECT unnest(?::" + target + ") AS v, generate_subscripts(?::" + target + ", 1) AS ord)"
}

func TestBindListElementCastEdge(t *testing.T) {
	type args struct {
		target string
		value  any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "bigint elements to integer", args: args{"INTEGER[]", []int64{1, 2}}, want: "1,2|2"},
		{name: "integer elements to smallint", args: args{"SMALLINT[]", []int32{1, 2}}, want: "1,2|2"},
		{name: "integer elements to bigint", args: args{"BIGINT[]", []int32{1, 2}}, want: "1,2|2"},
		{name: "bigint elements to double", args: args{"DOUBLE[]", []int64{1, 2}}, want: "1.0,2.0|2"},
		{name: "double elements to integer", args: args{"INTEGER[]", []float64{1, 2}}, want: "1,2|2"},
		{name: "bigint elements to varchar", args: args{"VARCHAR[]", []int64{1, 2}}, want: "1,2|2"},
		{name: "integer elements to boolean", args: args{"BOOLEAN[]", []int32{0, 1}}, want: "false,true|2"},
		{name: "integer max to integer", args: args{"INTEGER[]", []int64{math.MaxInt32, math.MinInt32}}, want: "2147483647,-2147483648|2"},
		{name: "bigint above integer range", args: args{"INTEGER[]", []int64{math.MaxInt32 + 1}}, wantErr: true},
		{name: "integer above smallint range", args: args{"SMALLINT[]", []int32{math.MaxInt16 + 1}}, wantErr: true},
		{name: "null element", args: args{"INTEGER[]", []any{int64(1), nil}}, want: "1|2"},
		{name: "all null elements", args: args{"INTEGER[]", []any{nil, nil}}, want: "|2"},
		{name: "empty list", args: args{"INTEGER[]", []int64{}}, want: "|0"},
		{name: "matching go type keeps target binding", args: args{"INTEGER[]", []int32{1, 2}}, want: "1,2|2"},
		{name: "pointer elements to integer", args: args{"INTEGER[]", []*int64{ptrTo(int64(3)), nil}}, want: "3|2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, "")
			defer closeDbWrapper(t, db)
			var got string
			err := db.QueryRow(unnestCastQuery(tt.args.target), tt.args.value, tt.args.value).Scan(&got)
			if tt.wantErr {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "interface conversion", "a type assertion panic must not surface")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// Nested lists: the mismatch is two levels down, so the whole value goes through the source binding. The value is
// inserted into a column of the target type: DuckDB resolves a parameter from a nested cast such as `?::INTEGER[][]`
// only as an unknown type, which binds by Go type inference and never reaches the declared-type path.
func TestBindListElementCastNested(t *testing.T) {
	type args struct {
		target string
		value  any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "bigint rows to integer matrix", args: args{"INTEGER[][]", [][]int64{{1, 2}, {3}}}, want: "[[1, 2], [3]]"},
		{name: "double rows to bigint matrix", args: args{"BIGINT[][]", [][]float64{{1}, {2}}}, want: "[[1], [2]]"},
		{name: "overflow inside a row", args: args{"INTEGER[][]", [][]int64{{1}, {math.MaxInt32 + 1}}}, wantErr: true},
		{name: "null row", args: args{"INTEGER[][]", []any{[]int64{4}, nil}}, want: "[[4], NULL]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, "")
			defer closeDbWrapper(t, db)
			_, err := db.Exec("CREATE TABLE nested(c " + tt.args.target + ")")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO nested VALUES (?)", tt.args.value)
			if tt.wantErr {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "interface conversion")
				return
			}
			require.NoError(t, err)
			var got string
			require.NoError(t, db.QueryRow("SELECT c::VARCHAR FROM nested").Scan(&got))
			require.Equal(t, tt.want, got)
		})
	}
}

// Property: over generated int64 lists bound to INTEGER[], every in-range list round-trips element for element and
// every list with an out-of-range element fails with an error, never a panic.
func TestBindListElementCastPropertySeeded(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	rng := rand.New(rand.NewSource(3238))
	inRange, outOfRange := 0, 0
	for i := range 300 {
		n := rng.Intn(5)
		values := make([]int64, n)
		overflow := false
		for j := range values {
			if rng.Intn(8) == 0 {
				values[j] = math.MaxInt32 + 1 + rng.Int63n(1<<20)
				overflow = true
			} else {
				values[j] = rng.Int63n(1<<31) - (1 << 30)
			}
		}
		var got string
		err := db.QueryRow(unnestCastQuery("INTEGER[]"), values, values).Scan(&got)
		if overflow {
			outOfRange++
			require.Error(t, err, "case %d: %v", i, values)
			require.NotContains(t, err.Error(), "interface conversion")
			continue
		}
		inRange++
		require.NoError(t, err, "case %d: %v", i, values)
		parts := make([]string, n)
		for j, v := range values {
			parts[j] = itoa64(v)
		}
		require.Equal(t, strings.Join(parts, ",")+"|"+itoa64(int64(n)), got, "case %d", i)
	}
	require.Positive(t, inRange)
	require.Positive(t, outOfRange)
}

// RoundTrip: a list bound through the cast fallback and read back as the declared type is the list we sent.
func TestBindListElementCastRoundTrip(t *testing.T) {
	type args struct {
		value []int64
	}
	tests := []struct {
		name    string
		args    args
		want    []int32
		wantErr bool
	}{
		{name: "two", args: args{[]int64{7, -8}}, want: []int32{7, -8}},
		{name: "bounds", args: args{[]int64{math.MaxInt32, math.MinInt32, 0}}, want: []int32{math.MaxInt32, math.MinInt32, 0}},
		{name: "empty", args: args{[]int64{}}, want: []int32{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, "")
			defer closeDbWrapper(t, db)
			var got []any
			err := db.QueryRow("SELECT ?::INTEGER[]", tt.args.value).Scan(&got)
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i, v := range got {
				require.Equal(t, tt.want[i], v)
			}
		})
	}
}

// errElementType is the only error the fallback reacts to: a bad value of the right Go type still fails as before.
func TestAssertElementTypeReportsMismatch(t *testing.T) {
	_, err := assertElementType[int32](TYPE_INTEGER, int64(1))
	require.ErrorIs(t, err, errElementType)
	v, err := assertElementType[int32](TYPE_INTEGER, int32(5))
	require.NoError(t, err)
	require.Equal(t, int32(5), v)
	require.NotErrorIs(t, castError("a", "b"), errElementType)
}

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }

// The fallback binds a mismatched value exactly as a parameter of unknown type binds it (tryBindComplexValue), and
// DuckDB casts it: for every value, inserting it through the declared-type path and through an unknown-type
// parameter gives the same stored value, or both fail. Values inference cannot bind are part of the table so that
// the fallback does not grow rules of its own.
func TestBindListElementCastMatchesUnknownTypeBinding(t *testing.T) {
	type args struct {
		target string
		value  any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "bigint elements", args: args{"INTEGER[]", []int64{1, 2}}, want: "[1, 2]"},
		{name: "double elements round half to even", args: args{"INTEGER[]", []float64{0.5, 1.5, 2.5}}, want: "[0, 2, 2]"},
		{name: "pointer elements", args: args{"INTEGER[]", []*int64{ptrTo(int64(3)), nil}}, want: "[3, NULL]"},
		{name: "null element", args: args{"INTEGER[]", []any{int64(1), nil}}, want: "[1, NULL]"},
		{name: "bigint rows", args: args{"INTEGER[][]", [][]int64{{1}, {2, 3}}}, want: "[[1], [2, 3]]"},
		{name: "stringer elements", args: args{"VARCHAR[]", []stringerCode{1}}, want: "[custom-1]"},
		{name: "overflow", args: args{"INTEGER[]", []int64{math.MaxInt32 + 1}}, wantErr: true},
		{name: "mixed element types", args: args{"INTEGER[]", []any{int64(1), int32(2)}}, wantErr: true},
		{name: "defined type without stringer", args: args{"INTEGER[]", []namedInt64{1}}, wantErr: true},
		{name: "byte rows read as varchar", args: args{"INTEGER[][]", [][]byte{{1}}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDbWrapper(t, "")
			defer closeDbWrapper(t, db)
			_, err := db.Exec("CREATE TABLE declared(c " + tt.args.target + "); CREATE TABLE unknown(c " + tt.args.target + ")")
			require.NoError(t, err)
			requireUnknownParamType(t, db, "SELECT x FROM (SELECT ? AS x)")
			_, declaredErr := db.Exec("INSERT INTO declared VALUES (?)", tt.args.value)
			_, unknownErr := db.Exec("INSERT INTO unknown SELECT x FROM (SELECT ? AS x)", tt.args.value)
			if declaredErr != nil {
				require.NotContains(t, declaredErr.Error(), "interface conversion")
			}
			require.Equal(t, unknownErr != nil, declaredErr != nil, "declared: %v / unknown: %v", declaredErr, unknownErr)
			require.Equal(t, tt.wantErr, declaredErr != nil, "declared: %v", declaredErr)
			if tt.wantErr {
				return
			}
			var declared, unknown string
			require.NoError(t, db.QueryRow("SELECT c::VARCHAR FROM declared").Scan(&declared))
			require.NoError(t, db.QueryRow("SELECT c::VARCHAR FROM unknown").Scan(&unknown))
			require.Equal(t, tt.want, declared)
			require.Equal(t, unknown, declared)
		})
	}
}

// requireUnknownParamType checks that the query's first parameter really has no resolved type, so the comparison
// above measures the unknown-type path.
func requireUnknownParamType(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	require.NoError(t, conn.Raw(func(driverConn any) (retErr error) {
		stmt, err := driverConn.(*Conn).PrepareContext(context.Background(), query)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, stmt.Close()) }()
		typ, err := stmt.(*Stmt).ParamType(1)
		if err != nil {
			return err
		}
		require.Equal(t, TYPE_INVALID, typ)
		return nil
	}))
}

type (
	namedInt64   int64
	stringerCode int64
)

func (c stringerCode) String() string { return "custom-" + strconv.FormatInt(int64(c), 10) }

func ptrTo[T any](v T) *T { return &v }
