package duckdb

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	_, err := GetProfilingInfo(conn)
	require.ErrorContains(t, err, errProfilingInfoEmpty.Error())

	_, err = conn.ExecContext(ctx, `SET enable_profiling = 'no_output'`)
	require.NoError(t, err)

	query := `SELECT range AS i FROM range(100) ORDER BY i`
	res, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	info, err := GetProfilingInfo(conn)
	require.NoError(t, err)

	// The root has no metrics of its own. Its children are the operator tree,
	// followed by metric groups such as the query's.
	require.Empty(t, info.Metrics, "root metrics must be empty")
	require.NotEmpty(t, info.Children, "children must not be empty")
	require.NotEmpty(t, info.Children[0].Metrics, "operator metrics must not be empty")
	require.Equal(t, query, childMetric(t, info, "sql"))

	_, err = conn.ExecContext(ctx, `PRAGMA disable_profiling`)
	require.NoError(t, err)

	_, err = GetProfilingInfo(conn)
	require.ErrorContains(t, err, errProfilingInfoEmpty.Error())
}

func TestErrProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := GetProfilingInfo(conn)
	testError(t, err, errProfilingInfoEmpty.Error())
}

func TestCustomProfiling(t *testing.T) {
	defer func() {
		err := os.Remove("profile.db")
		require.NoError(t, err)
	}()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	_, err := conn.ExecContext(ctx, `PRAGMA disable_checkpoint_on_shutdown`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET checkpoint_threshold = '10.0 GB'`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `ATTACH 'profile.db'`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `CREATE TABLE profile.tbl (i INT)`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SET enable_profiling = 'no_output'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET profiling_coverage = 'ALL'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SET tracked_metrics = ['storage.checkpoint_latency']`)
	require.NoError(t, err)

	res, err := conn.QueryContext(ctx, `CHECKPOINT profile`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	info, err := GetProfilingInfo(conn)
	require.NoError(t, err)

	// Verify the metrics.
	f, err := strconv.ParseFloat(childMetric(t, info, "checkpoint_latency"), 64)
	require.NoError(t, err)
	require.Positive(t, f)
}

// childMetric returns the value of the metric key from the first child of info that has it.
func childMetric(t *testing.T, info ProfilingInfo, key string) string {
	t.Helper()
	for _, child := range info.Children {
		if value, ok := child.Metrics[key]; ok {
			return value
		}
	}
	require.Failf(t, "metric not found", "no child of the profiling info has %q", key)
	return ""
}
