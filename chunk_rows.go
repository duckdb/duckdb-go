package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// ChunkRows streams query results one DuckDB data chunk at a time.
// The returned chunk is invalidated by the next call to NextChunk or Close.
type ChunkRows struct {
	stmt *Stmt
	res  mapping.Result

	chunk      DataChunk
	closeChunk bool
}

func newChunkRowsWithStmt(res mapping.Result, stmt *Stmt) *ChunkRows {
	columnCount := mapping.ColumnCount(&res)
	r := &ChunkRows{
		res:   res,
		stmt:  stmt,
		chunk: DataChunk{},
	}

	for i := range uint64(columnCount) {
		columnName := mapping.ColumnName(&res, mapping.IdxT(i))
		r.chunk.columnNames = append(r.chunk.columnNames, columnName)
	}

	return r
}

// Columns returns the result column names.
func (r *ChunkRows) Columns() []string {
	return r.chunk.columnNames
}

// NextChunk advances to the next result chunk.
func (r *ChunkRows) NextChunk() (*DataChunk, error) {
	if r.closeChunk {
		r.chunk.close()
		r.closeChunk = false
	}

	chunk := mapping.FetchChunk(r.res)
	if chunk.Ptr == nil {
		return nil, io.EOF
	}

	r.closeChunk = true
	if err := r.chunk.initFromDuckDataChunk(chunk, false); err != nil {
		return nil, getError(err, nil)
	}

	return &r.chunk, nil
}

// Close releases the underlying result and prepared statement.
func (r *ChunkRows) Close() error {
	if r.closeChunk {
		r.chunk.close()
	}
	mapping.DestroyResult(&r.res)

	var err error
	if r.stmt != nil {
		r.stmt.rows = false
		if r.stmt.closeOnRowsClose {
			err = r.stmt.Close()
		}
		r.stmt = nil
	}

	return err
}

// QueryChunksContext executes a query and returns results chunk-by-chunk.
func (conn *Conn) QueryChunksContext(ctx context.Context, query string, args []driver.NamedValue) (*ChunkRows, error) {
	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	var rows *ChunkRows
	err := runWithCtxInterrupt(ctx, conn.conn, func(wctx context.Context) error {
		prepared, err := conn.prepareStmts(wctx, query)
		if err != nil {
			return err
		}

		r, err := prepared.QueryChunksContext(wctx, args)
		if err != nil {
			errClose := prepared.Close()
			if errClose != nil {
				return errors.Join(err, errClose)
			}
			return err
		}

		prepared.closeOnRowsClose = true
		rows = r
		return nil
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// QueryChunksContext executes a prepared query and returns results chunk-by-chunk.
func (s *Stmt) QueryChunksContext(ctx context.Context, nargs []driver.NamedValue) (*ChunkRows, error) {
	cleanupCtx := s.conn.setContext(ctx)
	defer cleanupCtx()

	var res *mapping.Result
	if err := runWithCtxInterrupt(ctx, s.conn.conn, func(wctx context.Context) error {
		var executeErr error
		res, executeErr = s.execute(wctx, nargs)
		return executeErr
	}); err != nil {
		return nil, err
	}

	s.rows = true
	return newChunkRowsWithStmt(*res, s), nil
}
