package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var currentInfo TypeInfo

type testCtxKeyType string

type counterKeyType struct{}

const (
	testCtxKey     testCtxKeyType = "test_ctx_key"
	testBindCtxKey testCtxKeyType = "test_bind_ctx_key"
)

type (
	simpleSUDF        struct{}
	constantSUDF      struct{}
	otherConstantSUDF struct{}
	typesSUDF         struct{}
	variadicSUDF      struct{}
	anyTypeSUDF       struct{}
	unionTestSUDF     struct{}
	getConnIdUDF      struct{}
	easterEggUDF      struct{}
	siblingUDF        struct{}
	errExecutorSUDF   struct{}
	errInputNilSUDF   struct{}
	errResultNilSUDF  struct{}
	errResultAnySUDF  struct{}
	errExecSUDF       struct{}
)

func simpleSum(values []driver.Value) (any, error) {
	if values[0] == nil || values[1] == nil {
		return nil, nil
	}
	val := values[0].(int32) + values[1].(int32)
	return val, nil
}

func constantOne([]driver.Value) (any, error) {
	return int32(1), nil
}

func identity(values []driver.Value) (any, error) {
	return values[0], nil
}

func variadicSum(values []driver.Value) (any, error) {
	sum := int32(0)
	for _, val := range values {
		if val == nil {
			return nil, nil
		}
		sum += val.(int32)
	}
	return sum, nil
}

func nilCount(values []driver.Value) (any, error) {
	count := int32(0)
	for _, val := range values {
		if val == nil {
			count++
		}
	}
	return count, nil
}

func constantError([]driver.Value) (any, error) {
	return nil, errors.New("test invalid execution")
}

func getConnId(ctx context.Context, values []driver.Value) (any, error) {
	if ctx == nil {
		return nil, errors.New("context is nil for getConnId")
	}

	id, ok := ctx.Value(testCtxKey).(uint64)
	if !ok {
		return nil, errors.New("context does not contain the connection id for getConnId")
	}

	return id, nil
}

func getEasterEgg(ctx context.Context, values []driver.Value) (any, error) {
	if ctx == nil {
		return nil, errors.New("context is nil for getEasterEgg")
	}

	// Ensure we have two input arguments.
	if len(values) != 2 {
		return nil, errors.New("invalid argument count")
	}

	// Get the custom bind data.
	anyCustomBindData := ctx.Value(testBindCtxKey)
	if anyCustomBindData == nil {
		return nil, errors.New("custom bind data is nil")
	}
	customBindData, ok := anyCustomBindData.(uint64)
	if !ok {
		return nil, errors.New("custom bind data must be uint64")
	}

	if values[0] != nil {
		customBindData += values[0].(uint64)
	}

	if customBindData == 42 {
		return "‧₊˚ ⋅ 𓐐𓎩 ‧₊˚ ⋅", nil
	}
	return strconv.Itoa(int(customBindData)), nil
}

func bindEasterEgg(parentCtx context.Context, args []ScalarUDFArg) (context.Context, error) {
	if !args[1].Foldable {
		return nil, errors.New("second argument must be foldable for bindEasterEgg")
	}
	if args[1].Value == nil {
		bindCtx := context.WithValue(parentCtx, testBindCtxKey, uint64(0))
		return bindCtx, nil
	}

	switch v := args[1].Value.(type) {
	case int32:
		bindCtx := context.WithValue(parentCtx, testBindCtxKey, uint64(v))
		return bindCtx, nil
	case uint64:
		bindCtx := context.WithValue(parentCtx, testBindCtxKey, v)
		return bindCtx, nil
	default:
		return nil, errors.New("cannot cast second argument to uint64")
	}
}

func (*simpleSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo, currentInfo}, currentInfo, nil, false, false}
}

func (*simpleSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: simpleSum}
}

func (*constantSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{ResultTypeInfo: currentInfo}
}

func (*constantSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantOne}
}

func (*otherConstantSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{}, currentInfo, nil, false, false}
}

func (*otherConstantSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantOne}
}

func (*typesSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo}, currentInfo, nil, false, false}
}

func (*typesSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: identity}
}

func (*variadicSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{nil, currentInfo, currentInfo, true, true}
}

func (*variadicSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: variadicSum}
}

func (*anyTypeSUDF) Config() ScalarFuncConfig {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}

	return ScalarFuncConfig{nil, currentInfo, info, false, true}
}

func (*anyTypeSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: nilCount}
}

func (*getConnIdUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{}, currentInfo, nil, true, false}
}

func (*getConnIdUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowContextExecutor: getConnId}
}

func (*easterEggUDF) Config() ScalarFuncConfig {
	info, err := NewTypeInfo(TYPE_UBIGINT)
	if err != nil {
		panic(err)
	}

	return ScalarFuncConfig{[]TypeInfo{info, info}, currentInfo, nil, true, true}
}

func (*easterEggUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowContextExecutor: getEasterEgg, ScalarBinder: bindEasterEgg}
}

func (*errExecutorSUDF) Config() ScalarFuncConfig {
	scalarUDF := simpleSUDF{}
	return scalarUDF.Config()
}

func (*errExecutorSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: nil}
}

func (*errInputNilSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{nil}, currentInfo, nil, false, false}
}

func (*errInputNilSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantOne}
}

func (*errResultNilSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo}, nil, nil, false, false}
}

func (*errResultNilSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantOne}
}

func (*errResultAnySUDF) Config() ScalarFuncConfig {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}

	return ScalarFuncConfig{[]TypeInfo{currentInfo}, info, nil, false, false}
}

func (*errResultAnySUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantOne}
}

func (*errExecSUDF) Config() ScalarFuncConfig {
	scalarUDF := simpleSUDF{}
	return scalarUDF.Config()
}

func (*errExecSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{RowExecutor: constantError}
}

func (*unionTestSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{currentInfo},
		ResultTypeInfo: currentInfo,
	}
}

func (*unionTestSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			return values[0], nil
		},
	}
}

func (u *siblingUDF) Config() ScalarFuncConfig {
	inputType, _ := NewTypeInfo(TYPE_BIGINT)
	resultType, _ := NewTypeInfo(TYPE_BIGINT)
	return ScalarFuncConfig{
		InputTypeInfos: []TypeInfo{inputType},
		ResultTypeInfo: resultType,
	}
}

func (u *siblingUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		// ScalarBinder is called once per expression during query planning.
		// The context is stored per-connection, but we need to ensure that the second call does not see the first call's state.
		ScalarBinder: func(parentCtx context.Context, args []ScalarUDFArg) (context.Context, error) {
			// Get the current counter from the context (default is 0).
			counter, _ := parentCtx.Value(counterKeyType{}).(int)
			counter++
			// Return an updated context with the new counter.
			return context.WithValue(parentCtx, counterKeyType{}, counter), nil
		},

		// RowContextExecutor is called for each row during query execution.
		// It receives the context stored through its specific binder.
		RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
			counter := ctx.Value(counterKeyType{}).(int)
			arg := values[0].(int64)
			res := arg * int64(counter)
			return res, nil
		},
	}
}

func TestSimpleScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *simpleSUDF
	err = RegisterScalarUDF(conn, "my_sum", udf)
	require.NoError(t, err)

	var sum *int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 52, *sum)

	row = db.QueryRow(`SELECT my_sum(NULL, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_sum(42, NULL) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)
}

func TestConstantScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *constantSUDF
	err = RegisterScalarUDF(conn, "constant_one", udf)
	require.NoError(t, err)

	var otherUDF *otherConstantSUDF
	err = RegisterScalarUDF(conn, "other_constant_one", otherUDF)
	require.NoError(t, err)

	var one int
	row := db.QueryRow(`SELECT constant_one() AS one`)
	require.NoError(t, row.Scan(&one))
	require.Equal(t, 1, one)

	row = db.QueryRow(`SELECT other_constant_one() AS one`)
	require.NoError(t, row.Scan(&one))
	require.Equal(t, 1, one)
}

func TestAllTypesScalarUDF(t *testing.T) {
	typeInfos := getTypeInfos(t, false)
	for _, info := range typeInfos {
		func() {
			currentInfo = info.TypeInfo

			db := openDbWrapper(t, ``)
			defer closeDbWrapper(t, db)

			_, err := db.Exec(`SET TimeZone = 'UTC'`)
			require.NoError(t, err)

			conn := openConnWrapper(t, db, context.Background())
			defer closeConnWrapper(t, conn)

			_, err = conn.ExecContext(context.Background(), `CREATE TYPE greeting AS ENUM ('hello', 'world')`)
			require.NoError(t, err)

			var udf *typesSUDF
			err = RegisterScalarUDF(conn, "my_identity", udf)
			require.NoError(t, err)

			var res any
			row := db.QueryRow(fmt.Sprintf(`SELECT my_identity(%s) AS res`, info.input))
			require.NoError(t, row.Scan(&res))
			if info.InternalType() != TYPE_UUID {
				require.Equal(t, info.output, fmt.Sprint(res), `output does not match expected output, input: %s`, info.input)
			} else {
				require.NotEmpty(t, res, "uuid empty")
			}
		}()
	}
}

func TestScalarUDFSet(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf1 *simpleSUDF
	var udf2 *typesSUDF
	err = RegisterScalarUDFSet(conn, "my_addition", udf1, udf2)
	require.NoError(t, err)

	var sum int
	row := db.QueryRow(`SELECT my_addition(10, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 52, sum)

	row = db.QueryRow(`SELECT my_addition(42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 42, sum)
}

func TestVariadicScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *variadicSUDF
	err = RegisterScalarUDF(conn, "my_variadic_sum", udf)
	require.NoError(t, err)

	var sum *int
	row := db.QueryRow(`SELECT my_variadic_sum(10, NULL, NULL) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_variadic_sum(10, 42, 2, 2, 2) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 58, *sum)

	row = db.QueryRow(`SELECT my_variadic_sum(10) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 10, *sum)

	row = db.QueryRow(`SELECT my_variadic_sum(NULL) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_variadic_sum() AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 0, *sum)
}

func TestANYScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *anyTypeSUDF
	err = RegisterScalarUDF(conn, "my_null_count", udf)
	require.NoError(t, err)

	var count int
	row := db.QueryRow(`SELECT my_null_count(10, 'hello', 2, [2], 2) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	row = db.QueryRow(`SELECT my_null_count(10, NULL, NULL, [NULL], {'hello': NULL}) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 2, count)

	row = db.QueryRow(`SELECT my_null_count(10, True) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	row = db.QueryRow(`SELECT my_null_count(NULL) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count)

	row = db.QueryRow(`SELECT my_null_count() AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)
}

func TestUnionScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := conn.ExecContext(context.Background(), `CREATE TYPE number_or_string AS UNION(number INTEGER, text VARCHAR)`)
	require.NoError(t, err)

	// Create member types.
	intInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	varcharInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	// Create UNION type info.
	unionInfo, err := NewUnionInfo(
		[]TypeInfo{intInfo, varcharInfo},
		[]string{"number", "text"},
	)
	require.NoError(t, err)

	currentInfo = unionInfo

	var udf *unionTestSUDF
	err = RegisterScalarUDF(conn, "union_identity", udf)
	require.NoError(t, err)

	// Test with integer input.
	var res any
	row := db.QueryRow(`SELECT union_identity(42::number_or_string) AS res`)
	require.NoError(t, row.Scan(&res))
	require.Equal(t, Union{Value: int32(42), Tag: "number"}, res)

	// Test with string input.
	row = db.QueryRow(`SELECT union_identity('hello'::number_or_string) AS res`)
	require.NoError(t, row.Scan(&res))
	require.Equal(t, Union{Value: "hello", Tag: "text"}, res)

	// Test with NULL input.
	row = db.QueryRow(`SELECT union_identity(NULL::number_or_string) AS res`)
	require.NoError(t, row.Scan(&res))
	require.Nil(t, res)
}

func TestGetConnIdScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn1 := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn1)
	conn1Id, err := ConnId(conn1)
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_UBIGINT)
	require.NoError(t, err)

	var udf *getConnIdUDF
	err = RegisterScalarUDF(conn1, "get_conn_id", udf)
	require.NoError(t, err)

	conn2 := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn2)
	conn2Id, err := ConnId(conn2)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), testCtxKey, conn1Id)

	var connId uint64
	row := conn1.QueryRowContext(ctx, `SELECT get_conn_id() AS connId`)
	require.NoError(t, row.Scan(&connId))
	require.Equal(t, conn1Id, connId)

	ctx = context.WithValue(context.Background(), testCtxKey, conn2Id)

	row = conn2.QueryRowContext(ctx, `SELECT get_conn_id() AS connId`)
	require.NoError(t, row.Scan(&connId))
	require.Equal(t, conn2Id, connId)

	var res bool
	row = conn2.QueryRowContext(ctx, fmt.Sprintf(`SELECT true AS res WHERE get_conn_id() = %d`, conn2Id))
	require.NoError(t, row.Scan(&res))
	require.True(t, res)
}

func TestGetConnIdScalarUDFPrepared(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)
	connId, err := ConnId(conn)
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_UBIGINT)
	require.NoError(t, err)

	var udf *getConnIdUDF
	err = RegisterScalarUDF(conn, "get_conn_id", udf)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), testCtxKey, connId)

	var connIdRes uint64
	stmt, err := conn.PrepareContext(ctx, `SELECT get_conn_id() AS connId`)
	require.NoError(t, err)
	defer closePreparedWrapper(t, stmt)

	row := stmt.QueryRowContext(ctx)
	require.NoError(t, row.Scan(&connIdRes))
	require.Equal(t, connId, connIdRes)
}

func TestBindScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	var udf *easterEggUDF
	err = RegisterScalarUDF(conn, "find_easter_egg", udf)
	require.NoError(t, err)

	var egg string
	row := conn.QueryRowContext(context.Background(), `SELECT find_easter_egg(7, 8) AS egg`)
	require.NoError(t, row.Scan(&egg))
	require.Equal(t, "15", egg)

	row = conn.QueryRowContext(context.Background(), `SELECT find_easter_egg(10, (44 - 12)::UBIGINT) AS egg`)
	require.NoError(t, row.Scan(&egg))
	require.Equal(t, "‧₊˚ ⋅ 𓐐𓎩 ‧₊˚ ⋅", egg)

	row = conn.QueryRowContext(context.Background(), `SELECT find_easter_egg(NULL, NULL) AS egg`)
	require.NoError(t, row.Scan(&egg))
	require.Equal(t, "0", egg)

	row = conn.QueryRowContext(context.Background(), `SELECT find_easter_egg(7, (random() * 100)::UBIGINT) AS egg`)
	err = row.Scan(&egg)
	require.ErrorContains(t, err, "second argument must be foldable for bindEasterEgg")
}

func TestSiblingUDFs(t *testing.T) {
	// SiblingUDF is used to test non-shared state behavior across multiple UDF calls in one query.
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf *siblingUDF
	err := RegisterScalarUDF(conn, "sibling_udf", udf)
	require.NoError(t, err)

	query := `SELECT sibling_udf(1) + sibling_udf(2) AS res`

	var res int64
	err = conn.QueryRowContext(context.Background(), query).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, int64(3), res)
}

func TestErrScalarUDF(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	// Empty name.
	var emptyNameUDF *simpleSUDF
	err = RegisterScalarUDF(conn, "", emptyNameUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoName.Error())

	// Invalid executor.
	var errExecutorUDF *errExecutorSUDF
	err = RegisterScalarUDF(conn, "err_executor_is_nil", errExecutorUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoExecutor.Error())

	// Invalid input parameter.
	var errInputNilUDF *errInputNilSUDF
	err = RegisterScalarUDF(conn, "err_input_type_is_nil", errInputNilUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFInputTypeIsNil.Error())

	// Invalid result parameters.
	var errResultNil *errResultNilSUDF
	err = RegisterScalarUDF(conn, "err_result_type_is_nil", errResultNil)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsNil.Error())
	var errResultAny *errResultAnySUDF
	err = RegisterScalarUDF(conn, "err_result_type_is_any", errResultAny)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsANY.Error())

	// Error during execution.
	var errExecUDF *errExecSUDF
	err = RegisterScalarUDF(conn, "err_exec", errExecUDF)
	require.NoError(t, err)
	row := db.QueryRow(`SELECT err_exec(10, 10) AS res`)
	testError(t, row.Err(), errAPI.Error())

	// Register the same scalar function a second time.
	// Since RegisterScalarUDF takes ownership of udf, we are now passing nil.
	var udf *simpleSUDF
	err = RegisterScalarUDF(conn, "my_sum", udf)
	require.NoError(t, err)
	err = RegisterScalarUDF(conn, "my_sum", udf)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function whose name already exists.
	var errDuplicateUDF *simpleSUDF
	err = RegisterScalarUDF(conn, "my_sum", errDuplicateUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function that is nil.
	err = RegisterScalarUDF(conn, "my_sum", nil)
	testError(t, err, errAPI.Error(), errScalarUDFIsNil.Error())
}

func TestErrScalarUDFClosedConn(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var err error
	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, context.Background())
	closeConnWrapper(t, conn)

	var errClosedConUDF *simpleSUDF
	err = RegisterScalarUDF(conn, "closed_con", errClosedConUDF)
	require.ErrorContains(t, err, sql.ErrConnDone.Error())
}
