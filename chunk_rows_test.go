package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryChunksContextStreamsChunkValues(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE numbers (i INTEGER)`)

	_, err := db.ExecContext(ctx, `INSERT INTO numbers VALUES (1), (2), (3)`)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	var got []int32
	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT i FROM numbers ORDER BY i`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		for {
			chunk, err := rows.NextChunk()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			for rowIdx := range chunk.GetSize() {
				value, err := chunk.GetValue(0, rowIdx)
				require.NoError(t, err)
				got = append(got, value.(int32))
			}
		}
	})
	require.NoError(t, err)
	require.Equal(t, []int32{1, 2, 3}, got)
}

func TestQueryChunksContextAdvancesAcrossMultipleChunks(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE numbers (i INTEGER)`)

	totalRows := GetDataChunkCapacity() + 17
	insertSQL := `INSERT INTO numbers VALUES `
	for i := range totalRows {
		if i > 0 {
			insertSQL += ", "
		}
		insertSQL += fmt.Sprintf("(%d)", i)
	}

	_, err := db.ExecContext(ctx, insertSQL)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	var (
		chunkCount int
		rowCount   int
	)
	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT i FROM numbers ORDER BY i`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		for {
			chunk, err := rows.NextChunk()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			chunkCount++
			rowCount += chunk.GetSize()
		}
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, chunkCount, 2)
	require.Equal(t, totalRows, rowCount)
}

func TestQueryChunksContextTypedSlices(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE metrics (i INTEGER, f DOUBLE, ts BIGINT)`)

	_, err := db.ExecContext(ctx, `INSERT INTO metrics VALUES (1, 1.5, 10), (2, 2.5, 20), (3, 3.5, 30)`)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT i, f, ts FROM metrics ORDER BY i`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		chunk, err := rows.NextChunk()
		require.NoError(t, err)

		ints, err := chunk.Int32Slice(0)
		require.NoError(t, err)
		floats, err := chunk.Float64Slice(1)
		require.NoError(t, err)
		timestamps, err := chunk.Int64Slice(2)
		require.NoError(t, err)

		require.Equal(t, []int32{1, 2, 3}, ints)
		require.Equal(t, []float64{1.5, 2.5, 3.5}, floats)
		require.Equal(t, []int64{10, 20, 30}, timestamps)

		return nil
	})
	require.NoError(t, err)
}

func TestQueryChunksContextTypedSliceDetectsNulls(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE metrics (i INTEGER)`)

	_, err := db.ExecContext(ctx, `INSERT INTO metrics VALUES (1), (NULL), (3)`)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT i FROM metrics`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		chunk, err := rows.NextChunk()
		require.NoError(t, err)

		ints, err := chunk.Int32Slice(0)
		require.NoError(t, err)
		require.Len(t, ints, 3)
		require.Equal(t, int32(1), ints[0])
		require.Equal(t, int32(3), ints[2])

		isNull, err := chunk.IsNull(0, 1)
		require.NoError(t, err)
		require.True(t, isNull)

		isNull, err = chunk.IsNull(0, 0)
		require.NoError(t, err)
		require.False(t, isNull)

		return nil
	})
	require.NoError(t, err)
}

func TestQueryChunksContextTypedSliceRejectsWrongType(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	err := conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT 1.5::DOUBLE AS f`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		chunk, err := rows.NextChunk()
		require.NoError(t, err)

		_, err = chunk.Int32Slice(0)
		require.ErrorContains(t, err, "expected INTEGER")
		require.ErrorContains(t, err, "got DOUBLE")

		return nil
	})
	require.NoError(t, err)
}

func TestQueryChunksContextStringRefs(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE labels (s VARCHAR)`)

	_, err := db.ExecContext(ctx, `INSERT INTO labels VALUES ('alpha'), (NULL), ('gamma')`)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT s FROM labels ORDER BY s NULLS FIRST`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		chunk, err := rows.NextChunk()
		require.NoError(t, err)

		refs, err := chunk.StringRefs(0)
		require.NoError(t, err)
		require.Len(t, refs, 3)

		isNull, err := chunk.IsNull(0, 0)
		require.NoError(t, err)
		require.True(t, isNull)

		isNull, err = chunk.IsNull(0, 1)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, "alpha", refs[1].UnsafeString())
		require.Equal(t, 5, refs[1].Len())

		isNull, err = chunk.IsNull(0, 2)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, "gamma", refs[2].UnsafeString())

		return nil
	})
	require.NoError(t, err)
}

func TestQueryChunksContextTimestampMicrosSlice(t *testing.T) {
	connector := newConnectorWrapper(t, "", nil)
	defer closeConnectorWrapper(t, connector)

	db := sql.OpenDB(connector)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE TABLE events (ts TIMESTAMP)`)

	_, err := db.ExecContext(ctx, `INSERT INTO events VALUES (TIMESTAMP '2024-02-03 01:02:03.123456'), (TIMESTAMP '2024-02-03 01:02:04.654321')`)
	require.NoError(t, err)

	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	err = conn.Raw(func(driverConn any) error {
		duckConn := driverConn.(*Conn)

		rows, err := duckConn.QueryChunksContext(ctx, `SELECT ts FROM events ORDER BY ts`, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		chunk, err := rows.NextChunk()
		require.NoError(t, err)

		micros, err := chunk.TimestampMicrosSlice(0)
		require.NoError(t, err)
		require.Equal(t, []int64{1706922123123456, 1706922124654321}, micros)

		return nil
	})
	require.NoError(t, err)
}
