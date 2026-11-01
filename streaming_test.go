package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamingResultsReadsEveryRow(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()), `SELECT i FROM range(1000000) t(i)`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, rows)
	var n, sum, v int64
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
		sum += v
	}
	require.NoError(t, rows.Err())
	require.Equal(t, int64(1000000), n)
	require.Equal(t, int64(499999500000), sum)
}

// A streaming query that fails part-way through must report the failure, not end early.
func TestStreamingResultsReportsAFailurePartWay(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	rows, err := db.QueryContext(WithStreamingResults(context.Background()),
		`SELECT CASE WHEN i = 600000 THEN error('boom') ELSE i END FROM range(1000000) t(i)`)
	require.NoError(t, err, "the failure is past the first chunk, so it surfaces while fetching")
	defer closeRowsWrapper(t, rows)
	var n, v int64
	for rows.Next() {
		require.NoError(t, rows.Scan(&v))
		n++
	}
	require.ErrorContains(t, rows.Err(), "boom")
	require.Less(t, n, int64(600000))
}

func TestQueryWithoutStreamingReportsAFailureUpFront(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.QueryContext(context.Background(),
		`SELECT CASE WHEN i = 600000 THEN error('boom') ELSE i END FROM range(1000000) t(i)`)
	require.ErrorContains(t, err, "boom")
}
