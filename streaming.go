package duckdb

import (
	"context"
	"errors"
	"io"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

type streamingResultsKey struct{}

// WithStreamingResults returns a context under which QueryContext returns a streaming result.
// DuckDB then runs the query while its rows are fetched, instead of materialising the whole
// result before QueryContext returns. ExecContext is not affected.
//
// The driver holds one chunk of a streaming result at a time, so reading a large result does
// not need memory for all of its rows. Operators that need their whole input, such as sorts,
// aggregates, and the build side of joins, still use memory as usual.
//
// As the query runs during the fetch, an error part-way through is reported by Rows.Next and
// Rows.Err, not by QueryContext. Cancelling ctx interrupts the query while it is fetched.
//
// Until the rows are closed, the query occupies its connection, and other statements on that
// connection (such as on the same sql.Tx or sql.Conn) fail.
//
// Closing the rows before reading all of them runs the rest of the query, so that its side
// effects, such as nextval() or an INSERT in a CTE, apply just as without streaming. This
// can take as long as reading the remaining rows. A failure is reported by Rows.Close,
// Rows.Err, and Row.Scan. Inside a transaction, it also makes Commit fail.
// Rolling back a transaction closes its open rows in the same way.
// To abandon the rest of the query instead, cancel ctx or the context of the transaction:
// the unread work does not run.
func WithStreamingResults(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamingResultsKey{}, true)
}

func streamingResults(ctx context.Context) bool {
	on, _ := ctx.Value(streamingResultsKey{}).(bool)
	return on
}

// stream keeps the scope of a streaming query alive until its rows are closed:
// the context visible to UDFs, and the interrupter that propagates cancellation to DuckDB.
type stream struct {
	conn *Conn
	// ctx is the context of the query.
	ctx context.Context //nolint:containedctx
	// txCtx is the context of the transaction that the query runs in, or nil.
	txCtx context.Context //nolint:containedctx
	// stopInterrupt stops the interrupter of the query and waits for it to finish.
	stopInterrupt func()
	// clearCtx removes the query context from the connection's context store.
	clearCtx func()
}

// newStream takes over the scope of a streaming query.
// ctx must be the context passed to fn by runWithCtxInterrupt.
func (conn *Conn) newStream(ctx context.Context) *stream {
	s := &stream{
		conn:          conn,
		ctx:           ctx,
		stopInterrupt: keepCtxInterrupt(ctx),
		// Replace the context stored by the caller, whose cleanup runs when the query returns.
		clearCtx: conn.ctxStore.store(conn.id, ctx, true),
	}
	if conn.tx {
		s.txCtx = conn.txCtx
	}
	return s
}

// canceled reports whether the query or its transaction was canceled.
func (s *stream) canceled() bool {
	return s.ctx.Err() != nil || (s.txCtx != nil && s.txCtx.Err() != nil)
}

// end ends the scope of the query. Afterward, the connection can run other statements.
func (s *stream) end() {
	s.stopInterrupt()
	s.clearCtx()
}

// fetchChunk returns the next chunk of res, or io.EOF after the last one.
// A streaming result runs its query while it is fetched, so a fetch can also fail.
// Without the error check, such a failure would look like the end of the result.
func fetchChunk(res *mapping.Result) (mapping.DataChunk, error) {
	chunk := mapping.FetchChunk(*res)
	if chunk.Ptr != nil {
		return chunk, nil
	}
	if msg := mapping.ResultError(res); msg != "" {
		return chunk, getDuckDBError(msg)
	}
	return chunk, io.EOF
}

// finishStream runs the unread rest of a streaming query, so that its side effects apply.
// It does not run it if the query failed, if it was canceled, or if all rows were read.
func (r *rows) finishStream() error {
	s := r.stream
	if r.eof || r.failed || s.canceled() {
		return nil
	}

	for {
		chunk, err := fetchChunk(&r.res)
		if errors.Is(err, io.EOF) {
			r.eof = true
			return nil
		}
		if err != nil {
			if s.canceled() {
				// The interrupter stopped the query: cancellation abandons it.
				return nil
			}
			if s.txCtx != nil {
				// Commit closes open rows without returning their error, so it must fail on its own.
				s.conn.txErr = errors.Join(s.conn.txErr, err)
			}
			return err
		}
		mapping.DestroyDataChunk(&chunk)
	}
}
