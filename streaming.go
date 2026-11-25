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
// Rows.Err, not by QueryContext. Cancelling ctx, or the context of the transaction that the
// query runs in, interrupts the query while it is fetched.
//
// Until the rows are closed, or read to the end of the result or a failed fetch, the query
// occupies its connection, and other statements on that connection (such as on the same
// sql.Tx or sql.Conn) fail. database/sql closes the rows at that point.
//
// Closing the rows before reading all of them runs the rest of the query, so that its side
// effects, such as nextval() or an INSERT in a CTE, apply just as without streaming. This
// can take as long as reading the remaining rows. A failure is reported by Rows.Close,
// Rows.Err, and Row.Scan. Inside a transaction, it also makes Commit fail.
// To abandon the rest of the query instead, cancel ctx or the context of the transaction,
// before or during the close: the unread work does not run, and the cancellation is reported
// in the same way as a failure, as the query did not complete.
// Closing the connection before the rows also abandons the rest: the rows then report
// a closed connection.
//
// database/sql closes the open rows of a sql.Tx before Commit and Rollback, so both first
// run the rest of their queries, as above. The driver ends a streaming result that is still
// open then, such as rows queried through the sql.Conn of the transaction, or an Arrow
// reader: Commit runs the rest of its query as well, and waits for it, and Rollback abandons
// it. These rows then report an error instead of the end of the result, unless Commit found
// no rows left to read.
//
// DuckDB does not stream every result, such as of INSERT ... RETURNING or EXPLAIN.
// Such a result is complete when QueryContext returns, and behaves as without streaming.
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
	// rows are the rows of the query, which hold its result.
	rows *rows
	// ctx is the context of the query. In a transaction, it is also canceled with the
	// transaction's context, see Conn.withTxCancel.
	ctx context.Context //nolint:containedctx
	// inTx is true if the query runs in a transaction.
	inTx bool
	// stopInterrupt stops the interrupter of the query and waits for it to finish.
	stopInterrupt func()
	// clearCtx removes the query context from the connection's context store.
	clearCtx func()
	// releaseTxCancel ends the link between ctx and the transaction's context.
	releaseTxCancel func()
	// ended is true once the scope has ended: the result was read to its end or a fetch
	// failed, or the rows or the connection were closed.
	ended bool
	// cause is the cancellation of the query when its scope ended, or nil.
	cause error
}

// txCancelScope links the context of a streaming query to the context of its transaction.
type txCancelScope struct {
	// release ends the link. It must run after the interrupter has stopped.
	release func()
	// kept is true once a streaming result has taken over the link.
	kept bool
}

// withTxCancel returns, for a streaming query in a transaction, a context with the values of
// ctx that is also canceled when the transaction's context is. A streaming query runs until
// its rows are closed, so canceling its transaction must interrupt it as well.
// The interrupter watches the context it is started with, so the outermost query call links
// the contexts before starting it, and the returned cleanup must run after it has stopped.
// The cleanup does nothing once a streaming result has kept the link, see keepTxCancel.
// The link is kept on the connection, not in ctx: ctx reaches UDFs, which may pass it to
// queries on other connections, and those must not take over or end the link.
func (conn *Conn) withTxCancel(ctx context.Context) (context.Context, func()) {
	txCtx := conn.txCtx
	if !conn.tx || txCtx == nil || txCtx.Done() == nil || !streamingResults(ctx) || conn.txCancel != nil {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(txCtx, func() { cancel(context.Cause(txCtx)) })
	scope := &txCancelScope{release: func() {
		stop()
		cancel(nil)
	}}
	conn.txCancel = scope

	return ctx, func() {
		conn.txCancel = nil
		if !scope.kept {
			scope.release()
		}
	}
}

// keepTxCancel keeps the link made by withTxCancel after the query call returns,
// and returns the function that ends it.
func (conn *Conn) keepTxCancel() func() {
	scope := conn.txCancel
	if scope == nil {
		return func() {}
	}
	scope.kept = true
	return scope.release
}

// newStream takes over the scope of a streaming query.
// ctx is the context of the query, linked to the transaction's context by withTxCancel,
// and wctx is the context passed to fn by runWithCtxInterrupt, which also carries the
// interrupter scope.
func (conn *Conn) newStream(ctx, wctx context.Context) *stream {
	s := &stream{
		conn:          conn,
		ctx:           ctx,
		inTx:          conn.tx,
		stopInterrupt: keepCtxInterrupt(wctx, conn.conn),
		// Keep the context that the outermost query call stored for UDFs, whose cleanup runs
		// when that call returns. It is the caller's context, as without streaming: UDFs may
		// keep it and use it after the rows are closed, so it must not carry the interrupter
		// scope or the link to the transaction's context, which end then.
		clearCtx:        conn.ctxStore.keep(conn.id),
		releaseTxCancel: conn.keepTxCancel(),
	}
	conn.stream = s
	return s
}

// canceled returns the cause if the query or its transaction was canceled, or nil.
// Once the scope has ended, it returns the cause at that time: ending the link to the
// transaction's context cancels ctx, and a later cancellation no longer affects the query.
func (s *stream) canceled() error {
	if s.ended {
		return s.cause
	}
	if s.ctx.Err() == nil {
		return nil
	}
	return context.Cause(s.ctx)
}

// end ends the scope of the query. Afterward, the connection can run other statements.
// It runs when the result was read to its end or a fetch failed, as the query is then
// complete, or else when the rows are closed, or when the connection is closed first.
func (s *stream) end() {
	if s.ended {
		return
	}
	s.cause = s.canceled()
	s.ended = true
	s.stopInterrupt()
	s.clearCtx()
	s.releaseTxCancel()
	if s.conn.stream == s {
		s.conn.stream = nil
	}
}

// endStreamAtTxEnd ends the open streaming result of the connection, if any, so that the
// connection can run the statement that ends its transaction. DuckDB rejects that statement
// while a result is open, which would leave the transaction open.
// database/sql closes the rows of a sql.Tx before Commit and Rollback, but not other results
// of the connection, such as rows queried through its sql.Conn.
// If finish is true, the rest of the query runs first, as when its rows are closed, and a
// failure makes Commit fail. Otherwise, the rest is abandoned.
// The rows remain open. After the chunk that they hold, they report errStreamTxEnded or the
// failure of the query, as they are not complete, unless no rows were left unread.
func (conn *Conn) endStreamAtTxEnd(finish bool) {
	s := conn.stream
	if s == nil {
		return
	}
	r := s.rows
	if finish {
		// finishStream keeps a failure in conn.txErr.
		_ = r.finishStream()
	}
	mapping.DestroyResult(&r.res)
	s.end()
	if r.failed || (r.eof && !r.unread) {
		return
	}
	r.eof = false
	r.failed = true
	r.fetchErr = errStreamTxEnded
}

// fetchChunk returns the next chunk of the result of r, or io.EOF after the last one.
// Once the result was read to its end or a fetch failed, it keeps returning io.EOF or that
// failure. A streaming query is then complete, so fetchChunk ends its scope: its connection
// can run other statements before the rows are closed, and canceling the query's context
// must not interrupt them.
// The result of a streaming query whose connection was closed can no longer be fetched.
func (r *rows) fetchChunk() (mapping.DataChunk, error) {
	if r.eof {
		return mapping.DataChunk{}, io.EOF
	}
	if r.failed {
		return mapping.DataChunk{}, r.fetchErr
	}

	var chunk mapping.DataChunk
	var err error
	if r.stream != nil && r.stream.ended {
		err = errClosedCon
	} else if chunk, err = fetchChunk(&r.res); err == nil {
		return chunk, nil
	}
	if errors.Is(err, io.EOF) {
		r.eof = true
	} else {
		r.failed = true
		r.fetchErr = err
	}
	if r.stream != nil {
		r.stream.end()
	}
	return chunk, err
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
// It does not run it if all rows were read or if a fetch already failed.
// Canceling the query or its transaction abandons the rest, and the unread work does not
// run: finishStream then reports the cancellation, so that a caller does not take the
// query as complete. Inside a transaction, a failure also makes Commit fail.
func (r *rows) finishStream() error {
	s := r.stream
	if r.eof || r.failed {
		return nil
	}
	if s.ended {
		// The connection was closed before the rows, so the rest of the query did not run.
		return errClosedCon
	}

	var err error
	for {
		// The interrupter only stops a fetch in progress, so check between chunks as well.
		if err = s.canceled(); err != nil {
			break
		}
		var chunk mapping.DataChunk
		chunk, err = r.fetchChunk()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if cause := s.canceled(); cause != nil {
				// The interrupter stopped the query.
				err = cause
			}
			break
		}
		mapping.DestroyDataChunk(&chunk)
		r.unread = true
	}

	if s.inTx {
		// Commit closes open rows without returning their error, so it must fail on its own.
		s.conn.txErr = errors.Join(s.conn.txErr, err)
	}
	return err
}
