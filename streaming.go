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
// Until the rows are closed, the query occupies its connection, and other statements on that
// connection (such as on the same sql.Tx or sql.Conn) fail.
//
// Closing the rows before reading all of them runs the rest of the query, so that its side
// effects, such as nextval() or an INSERT in a CTE, apply just as without streaming. This
// can take as long as reading the remaining rows. A failure is reported by Rows.Close,
// Rows.Err, and Row.Scan. Inside a transaction, it also makes Commit fail.
// Rolling back a transaction closes its open rows in the same way.
// To abandon the rest of the query instead, cancel ctx or the context of the transaction,
// before or during the close: the unread work does not run, and the cancellation is reported
// in the same way as a failure, as the query did not complete.
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
	return &stream{
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
}

// canceled returns the cause if the query or its transaction was canceled, or nil.
func (s *stream) canceled() error {
	if s.ctx.Err() == nil {
		return nil
	}
	return context.Cause(s.ctx)
}

// end ends the scope of the query. Afterward, the connection can run other statements.
func (s *stream) end() {
	s.stopInterrupt()
	s.clearCtx()
	s.releaseTxCancel()
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

	var err error
	for {
		// The interrupter only stops a fetch in progress, so check between chunks as well.
		if err = s.canceled(); err != nil {
			break
		}
		var chunk mapping.DataChunk
		chunk, err = fetchChunk(&r.res)
		if errors.Is(err, io.EOF) {
			r.eof = true
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
	}

	if s.inTx {
		// Commit closes open rows without returning their error, so it must fail on its own.
		s.conn.txErr = errors.Join(s.conn.txErr, err)
	}
	return err
}
