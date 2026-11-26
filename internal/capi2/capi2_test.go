package capi2_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/internal/capi2"
)

func closeOnCleanup(t *testing.T, h interface{ Close() error }) {
	t.Helper()
	t.Cleanup(func() { require.NoError(t, h.Close()) })
}

func newEnvironment(t *testing.T) *capi2.Environment {
	t.Helper()
	require.NoError(t, capi2.Bootstrap())
	env, err := capi2.NewEnvironment()
	require.NoError(t, err)
	closeOnCleanup(t, env)
	return env
}

func newInstance(t *testing.T, env *capi2.Environment, path string) *capi2.Instance {
	t.Helper()
	instance, err := env.NewInstance()
	require.NoError(t, err)
	closeOnCleanup(t, instance)
	require.NoError(t, instance.Attach(path))
	return instance
}

func connect(t *testing.T, instance *capi2.Instance) *capi2.Connection {
	t.Helper()
	conn, err := instance.Connect()
	require.NoError(t, err)
	closeOnCleanup(t, conn)
	return conn
}

func query(t *testing.T, conn *capi2.Connection, sql string) *capi2.Result {
	t.Helper()
	iterator, err := conn.Parse(sql)
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()
	statement, err := iterator.Next()
	require.NoError(t, err)
	require.NotNil(t, statement)
	defer func() { require.NoError(t, statement.Close()) }()
	end, err := iterator.Next()
	require.NoError(t, err)
	require.Nil(t, end)
	result, err := conn.Execute(statement)
	require.NoError(t, err)
	closeOnCleanup(t, result)
	return result
}

func execSQL(t *testing.T, conn *capi2.Connection, sql string) uint64 {
	t.Helper()
	result := query(t, conn, sql)
	changed, err := result.Drain()
	require.NoError(t, err)
	require.NoError(t, result.Close())
	return changed
}

func readInt64(t *testing.T, conn *capi2.Connection, sql string) int64 {
	t.Helper()
	result := query(t, conn, sql)
	chunk, err := result.Fetch()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	closeOnCleanup(t, chunk)
	size, err := chunk.Size()
	require.NoError(t, err)
	require.Equal(t, uint64(1), size)
	value, err := chunk.Int64(0, 0)
	require.NoError(t, err)
	require.NoError(t, chunk.Close())
	end, err := result.Fetch()
	require.NoError(t, err)
	require.Nil(t, end)
	require.NoError(t, result.Close())
	return value
}

func requireUnreadyHandles(t *testing.T, want string) {
	t.Helper()
	env, err := capi2.NewEnvironment()
	require.EqualError(t, err, want)
	require.Nil(t, env)
	instance, err := (&capi2.Environment{}).NewInstance()
	require.EqualError(t, err, want)
	require.Nil(t, instance)
	conn, err := (&capi2.Instance{}).Connect()
	require.EqualError(t, err, want)
	require.Nil(t, conn)
	iterator, err := (&capi2.Connection{}).Parse("SELECT 73")
	require.EqualError(t, err, want)
	require.Nil(t, iterator)
	statement, err := (&capi2.Statements{}).Next()
	require.EqualError(t, err, want)
	require.Nil(t, statement)
	result, err := (&capi2.Connection{}).Execute(&capi2.Statement{})
	require.EqualError(t, err, want)
	require.Nil(t, result)
	chunk, err := (&capi2.Result{}).Fetch()
	require.EqualError(t, err, want)
	require.Nil(t, chunk)
	changed, err := (&capi2.Result{}).Drain()
	require.EqualError(t, err, want)
	require.Zero(t, changed)
	size, err := (&capi2.Chunk{}).Size()
	require.EqualError(t, err, want)
	require.Zero(t, size)
	value, err := (&capi2.Chunk{}).Int64(0, 0)
	require.EqualError(t, err, want)
	require.Zero(t, value)
	for name, err := range map[string]error{
		"Environment.Close": (&capi2.Environment{}).Close(),
		"Instance.Attach":   (&capi2.Instance{}).Attach(":memory:"),
		"Instance.Close":    (&capi2.Instance{}).Close(),
		"Connection.Close":  (&capi2.Connection{}).Close(),
		"Statements.Close":  (&capi2.Statements{}).Close(),
		"Statement.Close":   (&capi2.Statement{}).Close(),
		"Result.Close":      (&capi2.Result{}).Close(),
		"Chunk.Close":       (&capi2.Chunk{}).Close(),
	} {
		require.EqualError(t, err, want, name)
	}
}

func TestBootstrap(t *testing.T) {
	if os.Getenv("CAPI2_BOOTSTRAP_CHILD") == "" {
		binary, err := os.Executable()
		require.NoError(t, err)
		cmd := exec.Command(binary, "-test.run=^TestBootstrap$")
		cmd.Env = append(os.Environ(), "CAPI2_BOOTSTRAP_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	// A fresh process proves that import did not initialize the package.
	env, err := capi2.NewEnvironment()
	require.ErrorContains(t, err, "call Bootstrap")
	require.Nil(t, env)
	requireUnreadyHandles(t, err.Error())
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- capi2.Bootstrap()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, capi2.Bootstrap())
	conn := connect(t, newInstance(t, newEnvironment(t), ":memory:"))
	require.Equal(t, int64(4), readInt64(t, conn, `SELECT count(*) FROM duckdb_extensions()
		WHERE loaded AND extension_name IN ('core_functions', 'json', 'parquet', 'icu')`))
	require.Equal(t, int64(0), readInt64(t, conn, `SELECT count(*) FROM duckdb_settings()
		WHERE name IN ('autoload_known_extensions', 'autoinstall_known_extensions') AND value != 'false'`))
	require.Equal(t, int64(1), readInt64(t, conn, `SELECT json_valid('{"a": 73}')::BIGINT`))
	require.Equal(t, int64(6), readInt64(t, conn, `SELECT list_sum([1, 2, 3])::BIGINT`))
}

func TestPersistenceAndConnectionRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	env := newEnvironment(t)
	instance := newInstance(t, env, path)
	conn := connect(t, instance)
	execSQL(t, conn, "CREATE TABLE probe(value BIGINT)")
	require.Equal(t, uint64(1), execSQL(t, conn, "INSERT INTO probe VALUES (73)"))
	require.Equal(t, int64(73), readInt64(t, conn, "SELECT value FROM probe"))
	err := env.Close()
	var closeErr *capi2.Error
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, capi2.ResourceInUse, closeErr.Code)
	require.Contains(t, closeErr.Message, "destroying environment")
	require.NoError(t, conn.Close())
	require.NoError(t, instance.Close())
	// Positive control: all dependent handles are gone, in the SAME environment.
	reattached := newInstance(t, env, path)
	require.NoError(t, reattached.Close())
	require.NoError(t, env.Close())
	// New environment, persisted data rather than a live instance's cache.
	env = newEnvironment(t)
	instance = newInstance(t, env, path)
	conn = connect(t, instance)
	require.Equal(t, int64(73), readInt64(t, conn, "SELECT value FROM probe"))
	// Negative control: destroying an instance handle does not release a retained
	// connection. Environment destruction alone would not detect this leak.
	require.NoError(t, instance.Close())
	other, err := env.NewInstance()
	require.NoError(t, err)
	closeOnCleanup(t, other)
	err = other.Attach(path)
	var native *capi2.Error
	require.ErrorAs(t, err, &native)
	require.Contains(t, []capi2.Code{capi2.ResourceInUse, capi2.IOGeneral}, native.Code)
	require.NotEmpty(t, native.Message)
	t.Logf("retained connection rejected attachment: %v", err)
	require.Equal(t, int64(73), readInt64(t, conn, "SELECT value FROM probe"))
	require.NoError(t, conn.Close())
	require.NoError(t, other.Attach(path))
	conn = connect(t, other)
	require.Equal(t, int64(73), readInt64(t, conn, "SELECT value FROM probe"))
	require.NoError(t, conn.Close())
	require.NoError(t, other.Close())
	require.NoError(t, env.Close())
}

func TestExecutionErrorsAndChunkBoundaries(t *testing.T) {
	conn := connect(t, newInstance(t, newEnvironment(t), ":memory:"))
	iterator, err := conn.Parse("SELECT missing_column")
	require.NoError(t, err)
	statement, err := iterator.Next()
	require.NoError(t, err)
	require.NotNil(t, statement)
	closeOnCleanup(t, statement)
	require.NoError(t, iterator.Close())
	result, err := conn.Execute(statement)
	require.ErrorContains(t, err, "missing_column")
	require.Nil(t, result)
	iterator, err = conn.Parse("SELECT FROM")
	if err == nil {
		// The pinned parser defers syntax errors to iteration. The API permits
		// eager parsing too, so either boundary may report this error.
		statement, nextErr := iterator.Next()
		require.Nil(t, statement)
		err = nextErr
		require.NoError(t, iterator.Close())
	}
	require.Error(t, err)
	require.Equal(t, int64(73), readInt64(t, conn, "SELECT 73::BIGINT"))
	result = query(t, conn, "SELECT CAST(i::VARCHAR || 'x' AS BIGINT) FROM range(2) t(i)")
	chunk, err := result.Fetch()
	require.Error(t, err)
	require.Nil(t, chunk)
	require.NoError(t, result.Close())
	// Independent expected values cross the default engine chunk boundary.
	result = query(t, conn, "SELECT i * 3 - 7 FROM range(4099) t(i) ORDER BY i")
	var row uint64
	for {
		chunk, err = result.Fetch()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		size, sizeErr := chunk.Size()
		require.NoError(t, sizeErr)
		for offset := range size {
			value, valueErr := chunk.Int64(0, offset)
			require.NoError(t, valueErr)
			require.Equal(t, int64(row)*3-7, value)
			row++
		}
		require.NoError(t, chunk.Close())
	}
	require.Equal(t, uint64(4099), row)
	require.NoError(t, result.Close())
}

// The dynamic CI script runs only this test with a checksummed real 2.0.0
// library, including eager symbol binding where supported. No v1 Go import is
// allowed in this test binary: v1 initialization would run before the guard.
func TestRejectMismatchedLibrary(t *testing.T) {
	if os.Getenv("CAPI2_MISMATCH_TEST") == "" {
		t.Skip("requires the dynamic mismatch fixture")
	}
	for range 2 {
		err := capi2.Bootstrap()
		require.EqualError(t, err, "capi2: linked DuckDB v2.0.0, require v2.0.1")
		requireUnreadyHandles(t, err.Error())
	}
}
