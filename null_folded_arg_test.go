package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// These tests pin down that reading a folded or bound NULL argument never reaches a duckdb_get_* getter. Some getters
// throw a DuckDB InternalException on NULL (duckdb_get_varchar calls StringValue::Get), and the exception cannot unwind
// through the C API into a cgo callback, so the whole process terminates. Each scenario therefore runs in a child
// process: before the fix the child aborts, after it the child finishes and prints what the callback saw.

const nullArgChildEnv = "DUCKDB_GO_NULL_ARG_CHILD"

type nullArgCtxKey struct{}

// nullArgBinderUDF is a scalar UDF with special NULL handling and a binder, which is the combination that makes
// DuckDB fold a typed NULL constant argument and hand it to the bind callback.
type nullArgBinderUDF struct{}

func (*nullArgBinderUDF) Config() ScalarFuncConfig {
	anyType, _ := NewTypeInfo(TYPE_ANY)
	varchar, _ := NewTypeInfo(TYPE_VARCHAR)
	return ScalarFuncConfig{InputTypeInfos: []TypeInfo{anyType}, ResultTypeInfo: varchar, SpecialNullHandling: true}
}

func (*nullArgBinderUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{
		ScalarBinder: func(parent context.Context, args []ScalarUDFArg) (context.Context, error) {
			if len(args) != 1 || !args[0].Foldable || args[0].Value != nil {
				return nil, fmt.Errorf("expected one folded nil argument, got %+v", args)
			}
			return context.WithValue(parent, nullArgCtxKey{}, "folded=true value=<nil>"), nil
		},
		RowContextExecutor: func(ctx context.Context, _ []driver.Value) (any, error) {
			return ctx.Value(nullArgCtxKey{}), nil
		},
	}
}

// nullArgTableUDF records the positional and named arguments it was bound with.
type nullArgTableUDF struct {
	seen string
	done bool
}

func (u *nullArgTableUDF) ColumnInfos() []ColumnInfo {
	varchar, _ := NewTypeInfo(TYPE_VARCHAR)
	return []ColumnInfo{{Name: "seen", T: varchar}}
}

func (u *nullArgTableUDF) Init() {}

func (u *nullArgTableUDF) FillRow(row Row) (bool, error) {
	if u.done {
		return false, nil
	}
	u.done = true
	return true, SetRowValue(row, 0, u.seen)
}

func (u *nullArgTableUDF) Cardinality() *CardinalityInfo { return nil }

func (u *nullArgTableUDF) FillChunk(chunk DataChunk) error {
	if u.done {
		return chunk.SetSize(0)
	}
	u.done = true
	if err := chunk.SetValue(0, 0, u.seen); err != nil {
		return err
	}
	return chunk.SetSize(1)
}

func nullArgTableConfig(sqlType Type, namedOnly bool) TableFunctionConfig {
	argType, _ := NewTypeInfo(sqlType)
	if namedOnly {
		return TableFunctionConfig{NamedArguments: map[string]TypeInfo{"n": argType}}
	}
	return TableFunctionConfig{Arguments: []TypeInfo{argType}}
}

func nullArgTableBind(named map[string]any, args ...any) (*nullArgTableUDF, error) {
	if len(args) == 1 {
		if args[0] != nil {
			return nil, fmt.Errorf("expected nil positional argument, got %v", args[0])
		}
		return &nullArgTableUDF{seen: "arg=<nil>"}, nil
	}
	v, present := named["n"]
	if !present || v != nil {
		return nil, fmt.Errorf("expected nil named argument, got %v", named)
	}
	return &nullArgTableUDF{seen: "named=<nil>"}, nil
}

// TestNullFoldedArgChild runs one scenario when started as a child by the tests below; otherwise it does nothing.
func TestNullFoldedArgChild(t *testing.T) {
	mode := os.Getenv(nullArgChildEnv)
	if mode == "" {
		return
	}
	kind, sqlType, _ := strings.Cut(mode, ":")
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)
	var query string
	switch kind {
	case "scalar":
		require.NoError(t, RegisterScalarUDF(conn, "null_arg_probe", &nullArgBinderUDF{}))
		query = fmt.Sprintf(`SELECT null_arg_probe(NULL::%s)`, sqlType)
	case "row-pos", "row-named", "chunk-pos", "chunk-named":
		typeID, ok := nullArgTableTypes[sqlType]
		require.True(t, ok, sqlType)
		namedOnly := strings.HasSuffix(kind, "-named")
		config := nullArgTableConfig(typeID, namedOnly)
		if strings.HasPrefix(kind, "row-") {
			fn := RowTableFunction{Config: config, BindArguments: func(named map[string]any, args ...any) (RowTableSource, error) {
				return nullArgTableBind(named, args...)
			}}
			require.NoError(t, RegisterTableUDF(conn, "null_arg_table", fn))
		} else {
			fn := ChunkTableFunction{Config: config, BindArguments: func(named map[string]any, args ...any) (ChunkTableSource, error) {
				return nullArgTableBind(named, args...)
			}}
			require.NoError(t, RegisterTableUDF(conn, "null_arg_table", fn))
		}
		argument := fmt.Sprintf("NULL::%s", sqlType)
		if namedOnly {
			argument = "n := " + argument
		}
		query = fmt.Sprintf(`SELECT seen FROM null_arg_table(%s)`, argument)
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	var got string
	require.NoError(t, conn.QueryRowContext(context.Background(), query).Scan(&got))
	fmt.Printf("CHILD_RESULT %s\n", got)
}

var nullArgTableTypes = map[string]Type{
	"VARCHAR": TYPE_VARCHAR, "BIGINT": TYPE_BIGINT, "DOUBLE": TYPE_DOUBLE, "DATE": TYPE_DATE,
	"TIMESTAMP": TYPE_TIMESTAMP, "INTERVAL": TYPE_INTERVAL, "BIT": TYPE_BIT, "HUGEINT": TYPE_HUGEINT,
}

func runNullArgChild(t *testing.T, mode string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNullFoldedArgChild$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), nullArgChildEnv+"="+mode)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestScalarBinderNullFoldedArgument(t *testing.T) {
	types := []string{
		"VARCHAR", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "HUGEINT", "UHUGEINT", "FLOAT", "DOUBLE",
		"DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
		"INTERVAL", "BIT", "BIGNUM", "BLOB", "DECIMAL(10,2)", "UUID", "INTEGER[]",
	}
	for _, sqlType := range types {
		t.Run(sqlType, func(t *testing.T) {
			out, err := runNullArgChild(t, "scalar:"+sqlType)
			require.NoError(t, err, "the child process must not terminate:\n%s", out)
			require.Contains(t, out, "CHILD_RESULT folded=true value=<nil>", "a typed NULL folds to a nil value:\n%s", out)
		})
	}
}

func TestTableUDFNullArguments(t *testing.T) {
	for _, mode := range []string{"row-pos", "row-named", "chunk-pos", "chunk-named"} {
		t.Run(mode, func(t *testing.T) {
			for sqlType := range nullArgTableTypes {
				t.Run(sqlType, func(t *testing.T) {
					out, err := runNullArgChild(t, mode+":"+sqlType)
					require.NoError(t, err, "the child process must not terminate:\n%s", out)
					want := "CHILD_RESULT arg=<nil>"
					if strings.HasSuffix(mode, "-named") {
						want = "CHILD_RESULT named=<nil>"
					}
					require.Contains(t, out, want, "NULL arguments bind as nil:\n%s", out)
				})
			}
		})
	}
}

func TestGetValueNullAndNonNull(t *testing.T) {
	null := mapping.CreateNullValue()
	defer mapping.DestroyValue(&null)
	got, err := getValue(null)
	require.NoError(t, err)
	require.Nil(t, got)

	text := createVarchar("abc")
	defer mapping.DestroyValue(&text)
	got, err = getValue(text)
	require.NoError(t, err)
	require.Equal(t, "abc", got)

	number := mapping.CreateInt64(42)
	defer mapping.DestroyValue(&number)
	got, err = getValue(number)
	require.NoError(t, err)
	require.Equal(t, int64(42), got)
}
