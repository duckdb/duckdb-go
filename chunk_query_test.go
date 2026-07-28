package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestQueryChunksContext(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	var chunks, rows int
	err := QueryChunksContext(t.Context(), conn,
		"SELECT i::VARCHAR FROM range(?) AS values(i)",
		func(chunk *DataChunk) error {
			chunks++
			for row := range chunk.GetSize() {
				value, valid, err := chunk.GetVarcharView(0, row)
				require.NoError(t, err)
				require.True(t, valid)
				require.Equal(t, strconv.Itoa(rows), string(value))
				rows++
			}
			return nil
		},
		5000,
	)
	require.NoError(t, err)
	require.Equal(t, 5000, rows)
	require.Greater(t, chunks, 1)
}

func TestQueryChunksContextViews(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	err := QueryChunksContext(t.Context(), conn,
		`SELECT NULL::VARCHAR, ''::VARCHAR, repeat('x', 32),
			NULL::BLOB, ''::BLOB, encode(repeat('x', 32)), 42::INTEGER`,
		func(chunk *DataChunk) error {
			value, valid, err := chunk.GetVarcharView(0, 0)
			require.NoError(t, err)
			require.False(t, valid)
			require.Nil(t, value)

			value, valid, err = chunk.GetVarcharView(1, 0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Empty(t, value)

			value, valid, err = chunk.GetVarcharView(2, 0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Equal(t, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", string(value))

			value, valid, err = chunk.GetBlobView(3, 0)
			require.NoError(t, err)
			require.False(t, valid)
			require.Nil(t, value)

			value, valid, err = chunk.GetBlobView(4, 0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Empty(t, value)

			value, valid, err = chunk.GetBlobView(5, 0)
			require.NoError(t, err)
			require.True(t, valid)
			require.Equal(t, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", string(value))

			_, _, err = chunk.GetBlobView(0, 0)
			require.Error(t, err)
			_, _, err = chunk.GetVarcharView(6, 0)
			require.Error(t, err)
			_, _, err = chunk.GetVarcharView(0, 1)
			require.Error(t, err)
			_, _, err = chunk.GetVarcharView(99, 0)
			require.ErrorContains(t, err, columnCountErrMsg)
			return nil
		},
	)
	require.NoError(t, err)
}

func TestQueryChunksContextColumnNames(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	err := QueryChunksContext(t.Context(), conn,
		"SELECT 1 AS id, 'x' AS label",
		func(chunk *DataChunk) error {
			require.Equal(t, []string{"id", "label"}, chunk.ColumnNames())

			name, err := chunk.ColumnName(0)
			require.NoError(t, err)
			require.Equal(t, "id", name)

			name, err = chunk.ColumnName(1)
			require.NoError(t, err)
			require.Equal(t, "label", name)

			_, err = chunk.ColumnName(2)
			require.Error(t, err)
			return nil
		},
	)
	require.NoError(t, err)
}

// TestQueryChunksContextInlineBoundary covers both sides of the VARCHAR inline
// length, where getBytesView switches from inlined data to an out-of-line pointer.
func TestQueryChunksContextInlineBoundary(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	for _, length := range []int{1, 11, 12, 13, 24} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			expected := strings.Repeat("x", length)
			err := QueryChunksContext(t.Context(), conn,
				"SELECT repeat('x', ?)::VARCHAR, encode(repeat('x', ?))",
				func(chunk *DataChunk) error {
					value, valid, err := chunk.GetVarcharView(0, 0)
					require.NoError(t, err)
					require.True(t, valid)
					require.Equal(t, expected, string(value))

					value, valid, err = chunk.GetBlobView(1, 0)
					require.NoError(t, err)
					require.True(t, valid)
					require.Equal(t, expected, string(value))
					return nil
				},
				length, length,
			)
			require.NoError(t, err)
		})
	}
}

// TestQueryChunksContextAfterClose verifies that a retained chunk fails cleanly.
func TestQueryChunksContextAfterClose(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	var leaked *DataChunk
	err := QueryChunksContext(t.Context(), conn, "SELECT 'value'::VARCHAR AS v",
		func(chunk *DataChunk) error {
			leaked = chunk
			return nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, leaked)

	_, _, err = leaked.GetVarcharView(0, 0)
	require.ErrorIs(t, err, errClosedChunk)
	_, _, err = leaked.GetBlobView(0, 0)
	require.ErrorIs(t, err, errClosedChunk)
	_, err = leaked.GetValue(0, 0)
	require.ErrorIs(t, err, errClosedChunk)
	_, err = leaked.ColumnName(0)
	require.ErrorIs(t, err, errClosedChunk)
	require.ErrorIs(t, leaked.SetValue(0, 0, "x"), errClosedChunk)
	require.ErrorIs(t, SetChunkValue(*leaked, 0, 0, "x"), errClosedChunk)
	require.ErrorIs(t, leaked.SetSize(1), errClosedChunk)
	require.Zero(t, leaked.GetSize())
	require.Zero(t, leaked.ColumnCount())
	require.Nil(t, leaked.ColumnNames())
}

func TestQueryChunksContextCancel(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var chunks int
	err := QueryChunksContext(ctx, conn, "SELECT i FROM range(100000) AS values(i)",
		func(*DataChunk) error {
			chunks++
			cancel()
			return nil
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, chunks)

	// The connection must remain usable after an interrupted query.
	var value string
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 'ready'").Scan(&value))
	require.Equal(t, "ready", value)
}

func TestQueryChunksContextCanceledBeforeCall(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	called := false
	err := QueryChunksContext(ctx, conn, "SELECT 1",
		func(*DataChunk) error { called = true; return nil },
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
}

// TestQueryChunksContextNoRows verifies that consume is not called for a result without rows.
func TestQueryChunksContextNoRows(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	for _, query := range []string{
		"SELECT 1 WHERE false",
		"CREATE TABLE no_rows (i INTEGER)",
	} {
		called := false
		err := QueryChunksContext(t.Context(), conn, query,
			func(*DataChunk) error { called = true; return nil },
		)
		require.NoError(t, err)
		require.False(t, called, query)
	}
}

// TestQueryChunksContextMultipleStmts verifies that all but the last statement
// run for their side effects, and that only the last one yields chunks.
func TestQueryChunksContextMultipleStmts(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	var rows int
	err := QueryChunksContext(t.Context(), conn,
		"CREATE TABLE multi (i INTEGER); INSERT INTO multi VALUES (1), (2); SELECT i FROM multi",
		func(chunk *DataChunk) error {
			rows += chunk.GetSize()
			require.Equal(t, []string{"i"}, chunk.ColumnNames())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
}

func TestQueryChunksContextArgs(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	t.Run("positional", func(t *testing.T) {
		err := QueryChunksContext(t.Context(), conn, "SELECT ?::INTEGER + ?::INTEGER",
			func(chunk *DataChunk) error {
				value, err := chunk.GetValue(0, 0)
				require.NoError(t, err)
				require.Equal(t, int32(3), value)
				return nil
			},
			1, 2,
		)
		require.NoError(t, err)
	})

	t.Run("named", func(t *testing.T) {
		err := QueryChunksContext(t.Context(), conn, "SELECT $lhs::INTEGER + $rhs::INTEGER",
			func(chunk *DataChunk) error {
				value, err := chunk.GetValue(0, 0)
				require.NoError(t, err)
				require.Equal(t, int32(3), value)
				return nil
			},
			sql.Named("lhs", 1), sql.Named("rhs", 2),
		)
		require.NoError(t, err)
	})
}

func TestQueryChunksContextQueryError(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	called := false
	err := QueryChunksContext(t.Context(), conn, "SELECT * FROM does_not_exist",
		func(*DataChunk) error { called = true; return nil },
	)
	require.Error(t, err)
	require.False(t, called)

	var value string
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 'ready'").Scan(&value))
	require.Equal(t, "ready", value)
}

func TestQueryChunksContextFetchError(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	patchVar(t, &mapping.FetchChunk, func(mapping.Result) mapping.DataChunk {
		return mapping.DataChunk{}
	})
	patchVar(t, &mapping.ResultError, func(*mapping.Result) string {
		return "Executor Error: fetch failed"
	})

	called := false
	err := QueryChunksContext(t.Context(), conn, "SELECT 1",
		func(*DataChunk) error { called = true; return nil },
	)
	require.NotErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "fetch failed")
	var duckErr *Error
	require.ErrorAs(t, err, &duckErr)
	require.Equal(t, ErrorTypeExecutor, duckErr.Type)
	require.False(t, called)
}

func TestQueryChunksContextCanceledDuringFetch(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	ctx, cancel := context.WithCancel(t.Context())
	patchVar(t, &mapping.FetchChunk, func(mapping.Result) mapping.DataChunk {
		cancel()
		return mapping.DataChunk{}
	})
	patchVar(t, &mapping.ResultError, func(*mapping.Result) string {
		return "Interrupt Error: interrupted"
	})

	called := false
	err := QueryChunksContext(ctx, conn, "SELECT 1",
		func(*DataChunk) error { called = true; return nil },
	)
	require.Equal(t, context.Canceled, err)
	require.False(t, called)
}

// TestQueryChunksContextConsumerPanic verifies that the panic propagates intact
// and the connection is discarded.
func TestQueryChunksContextConsumerPanic(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())

	require.PanicsWithValue(t, "consumer", func() {
		_ = QueryChunksContext(t.Context(), conn, "SELECT i FROM range(10000) AS values(i)",
			func(*DataChunk) error { panic("consumer") },
		)
	})

	// database/sql discards a connection whose Raw callback panicked.
	require.Error(t, conn.Close())
}

func TestQueryChunksContextConsumerError(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	expected := errors.New("stop")
	err := QueryChunksContext(t.Context(), conn, "SELECT 'value'",
		func(*DataChunk) error { return expected },
	)
	// Returned unchanged, so that a sentinel error can stop the iteration early.
	require.Equal(t, expected, err)

	var value string
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 'ready'").Scan(&value))
	require.Equal(t, "ready", value)
}

func TestQueryChunksContextRejectsNilConsumer(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	require.Error(t, QueryChunksContext(t.Context(), conn, "SELECT 1", nil))
}

func TestQueryChunksContextRejectsNilConnection(t *testing.T) {
	require.Error(t, QueryChunksContext(t.Context(), nil, "SELECT 1",
		func(*DataChunk) error { return nil },
	))
}

// TestQueryChunksContextArgConversion pins that args go through the same
// conversion database/sql applies, so a query accepts the same types on both
// paths. A named type like ID, for one, needs the default converter to reach
// int64.
func TestQueryChunksContextArgConversion(t *testing.T) {
	type ID int64

	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	tests := []struct {
		name  string
		query string
		arg   any
	}{
		{"named type", "SELECT ?::BIGINT = 42", ID(42)},
		{"nil", "SELECT ?::INTEGER IS NULL", nil},
		{"time", "SELECT ?::TIMESTAMP = '1970-01-01'::TIMESTAMP", time.Unix(0, 0).UTC()},
		{"named arg", "SELECT $v::INTEGER = 7", sql.Named("v", 7)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var viaRows bool
			require.NoError(t, conn.QueryRowContext(t.Context(), tc.query, tc.arg).Scan(&viaRows))
			require.True(t, viaRows)

			var viaChunks bool
			err := QueryChunksContext(t.Context(), conn, tc.query,
				func(chunk *DataChunk) error {
					value, err := chunk.GetValue(0, 0)
					require.NoError(t, err)
					viaChunks = value.(bool)
					return nil
				},
				tc.arg,
			)
			require.NoError(t, err)
			require.Equal(t, viaRows, viaChunks)
		})
	}
}

// TestQueryChunksContextRejectsInvalidArg covers the conversion failing
func TestQueryChunksContextRejectsInvalidArg(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	called := false
	err := QueryChunksContext(t.Context(), conn, "SELECT ?::INTEGER",
		func(*DataChunk) error { called = true; return nil },
		make(chan int),
	)
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "chan int")
	require.False(t, called)

	var value string
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 'ready'").Scan(&value))
	require.Equal(t, "ready", value)
}

// TestQueryChunksContextUnsupportedColumnType covers a chunk that cannot be
// initialized: it must be destroyed before the error is returned, so that the
// column type DuckDB does support keeps working on the same connection.
func TestQueryChunksContextUnsupportedColumnType(t *testing.T) {
	db := openDbWrapper(t, "")
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, t.Context())
	defer closeConnWrapper(t, conn)

	called := false
	err := QueryChunksContext(t.Context(), conn, "SELECT NULL::VARIANT",
		func(*DataChunk) error { called = true; return nil },
	)
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, unsupportedTypeErrMsg)
	require.False(t, called)

	var value string
	require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT 'ready'").Scan(&value))
	require.Equal(t, "ready", value)
}
