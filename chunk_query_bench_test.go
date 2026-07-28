package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

const benchChunkRowCount = 100_000

// benchChunkQuery serializes every row to a single VARCHAR column.
var benchChunkQuery = fmt.Sprintf(
	`SELECT to_json(r)::VARCHAR FROM (
		SELECT i AS id, i::VARCHAR AS name, i * 1.5 AS value FROM range(%d) AS values(i)
	) AS r`, benchChunkRowCount)

func setupChunkBench(b *testing.B) (*sql.Conn, []byte) {
	b.Helper()
	db := openDbWrapper(b, "")
	conn := openConnWrapper(b, db, context.Background())
	b.Cleanup(func() {
		closeConnWrapper(b, conn)
		closeDbWrapper(b, db)
	})
	return conn, make([]byte, 0, 128*1024)
}

// BenchmarkQueryChunks reads the result through the native chunk API.
func BenchmarkQueryChunks(b *testing.B) {
	conn, buf := setupChunkBench(b)

	b.ReportAllocs()
	for b.Loop() {
		buf = buf[:0]
		err := QueryChunksContext(context.Background(), conn, benchChunkQuery,
			func(chunk *DataChunk) error {
				for rowIdx := range chunk.GetSize() {
					value, valid, err := chunk.GetVarcharView(0, rowIdx)
					if err != nil {
						return err
					}
					if !valid {
						continue
					}
					if len(value)+1 > cap(buf)-len(buf) {
						_, _ = io.Discard.Write(buf)
						buf = buf[:0]
					}
					buf = append(buf, value...)
					buf = append(buf, '\n')
				}
				return nil
			},
		)
		require.NoError(b, err)
	}
}

// BenchmarkQueryRows reads the result through database/sql.
func BenchmarkQueryRows(b *testing.B) {
	conn, buf := setupChunkBench(b)

	b.ReportAllocs()
	for b.Loop() {
		buf = buf[:0]
		rows, err := conn.QueryContext(context.Background(), benchChunkQuery)
		require.NoError(b, err)

		var value string
		for rows.Next() {
			if err := rows.Scan(&value); err != nil {
				b.Fatal(err)
			}
			if len(value)+1 > cap(buf)-len(buf) {
				_, _ = io.Discard.Write(buf)
				buf = buf[:0]
			}
			buf = append(buf, value...)
			buf = append(buf, '\n')
		}
		require.NoError(b, rows.Err())
		require.NoError(b, rows.Close())
	}
}
