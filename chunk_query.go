package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// QueryChunksContext executes a query and passes each native result chunk to
// consume. It exists to read results without the per-row allocations of
// database/sql: values can be read straight out of DuckDB's vectors, e.g., via
// DataChunk.GetVarcharView.
//
// The chunks arrive in result order, and consume observes one chunk at a time.
// Returning an error from consume stops the iteration and returns that error
// unchanged, so a sentinel error stops the iteration early.
// consume is not called at all for a result without rows.
//
// The connection is occupied for the whole call. consume must therefore not
// issue queries on sqlConn, which would deadlock. Note that DuckDB materializes
// the result before the first chunk is fetched, so this API bounds the memory
// that the caller allocates, not the memory that DuckDB does.
//
// The same *DataChunk is reused for every chunk, and its C-allocated memory is
// released once consume returns. Neither the chunk nor anything aliasing it may
// be retained beyond that. args accept the same types as db.QueryContext, as
// they go through the same conversion. If query holds multiple statements, all
// but the last are executed, and only the last one yields chunks.
func QueryChunksContext(
	ctx context.Context,
	sqlConn *sql.Conn,
	query string,
	consume func(*DataChunk) error,
	args ...any,
) error {
	if sqlConn == nil {
		return getError(errAPI, errNilConn)
	}
	if consume == nil {
		return getError(errAPI, errNilChunkConsumer)
	}

	return sqlConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return getError(errAPI, invalidConnError(driverConn))
		}
		namedArgs, err := conn.convertChunkArgs(args)
		if err != nil {
			return err
		}
		return conn.queryChunksContext(ctx, query, namedArgs, consume)
	})
}

// convertChunkArgs applies the argument conversion that database/sql would have
// applied on its way to the driver: CheckNamedValue first, and the default
// converter for the values it skips. Binding args verbatim instead would accept
// a narrower set of types here than the same query accepts through
// db.QueryContext.
func (conn *Conn) convertChunkArgs(args []any) ([]driver.NamedValue, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nv := driver.NamedValue{Ordinal: i + 1, Value: arg}
		if named, ok := arg.(sql.NamedArg); ok {
			nv.Name, nv.Value = named.Name, named.Value
		}

		err := conn.CheckNamedValue(&nv)
		if errors.Is(err, driver.ErrSkip) {
			nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
		}
		if err != nil {
			return nil, getError(errAPI, addIndexToError(err, i+1))
		}
		namedArgs[i] = nv
	}
	return namedArgs, nil
}

// queryChunksContext prepares query on conn, streams its chunks to consume, and
// closes the statement before returning.
func (conn *Conn) queryChunksContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
	consume func(*DataChunk) error,
) error {
	if conn.closed {
		return errClosedCon
	}

	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	return runWithCtxInterrupt(ctx, conn.conn, func(wctx context.Context) (err error) {
		prepared, err := conn.prepareStmts(wctx, query)
		if err != nil {
			return err
		}

		defer func() {
			closeErr := prepared.Close()
			switch {
			case err != nil && closeErr != nil:
				err = errors.Join(err, closeErr)
			case closeErr != nil:
				err = closeErr
			}
		}()

		return consumePreparedChunks(wctx, prepared, args, consume)
	})
}

// consumePreparedChunks executes prepared and passes every result chunk to
// consume, one at a time. It owns all C-allocated memory involved: the result is
// destroyed on return, and each fetched chunk is freed after consume observes it.
func consumePreparedChunks(
	ctx context.Context,
	prepared *Stmt,
	args []driver.NamedValue,
	consume func(*DataChunk) error,
) error {
	result, err := prepared.execute(ctx, args)
	if err != nil {
		return err
	}
	defer mapping.DestroyResult(result)

	columnCount := mapping.ColumnCount(result)
	columnNames := make([]string, columnCount)
	for i := range columnNames {
		columnNames[i] = mapping.ColumnName(result, mapping.IdxT(i))
	}

	var chunk DataChunk
	chunk.columnNames = columnNames
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nativeChunk := mapping.FetchChunk(*result)
		if nativeChunk.Ptr == nil {
			if errMsg := mapping.ResultError(result); errMsg != "" {
				if err := ctx.Err(); err != nil {
					return err
				}
				return getDuckDBError(errMsg)
			}
			return ctx.Err()
		}
		if err := chunk.initFromDuckDataChunk(nativeChunk, false); err != nil {
			mapping.DestroyDataChunk(&nativeChunk)
			return getError(errAPI, err)
		}

		err := consumeResultChunk(&chunk, consume)
		if err != nil {
			return err
		}
	}
}

// consumeResultChunk passes chunk to consume and frees its C-allocated memory.
func consumeResultChunk(chunk *DataChunk, consume func(*DataChunk) error) error {
	defer chunk.close()
	return consume(chunk)
}
