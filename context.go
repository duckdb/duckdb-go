package duckdb

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// contextStore stores the thread-safe context of a connection.
type contextStore struct {
	m sync.Map
}

// newContextStore creates a new instance of ctxStore.
func newContextStore() *contextStore {
	return &contextStore{
		m: sync.Map{},
	}
}

// ctxEntry wraps a stored context, so that a cleanup deletes only its own entry.
type ctxEntry struct {
	ctx context.Context //nolint:containedctx
}

func (s *contextStore) load(connId uint64) context.Context {
	v, ok := s.m.Load(connId)
	if !ok {
		return context.Background()
	}
	entry, ok := v.(*ctxEntry)
	if !ok {
		return context.Background()
	}

	return entry.ctx
}

// store sets the context of a connection. Without replace, it keeps an existing context.
// The returned cleanup removes the stored context, unless another call replaced it since.
func (s *contextStore) store(connId uint64, ctx context.Context, replace bool) func() {
	if !replace {
		_, ok := s.m.Load(connId)
		if ok {
			return func() {}
		}
	}

	entry := &ctxEntry{ctx: ctx}
	s.m.Store(connId, entry)

	return func() {
		s.m.CompareAndDelete(connId, entry)
	}
}

func (s *contextStore) delete(connId uint64) {
	s.m.Delete(connId)
}

// How frequently to `duckdb_interrupt` after ctx cancellation while the DuckDB call is still executing.
var interruptInterval = 500 * time.Millisecond

// runWithCtxInterrupt runs the function fn (which runs DuckDB mapping call/s),
// and robustly propagates ctx cancellation to DuckDB by repeatedly calling
// mapping.Interrupt on the given connection while the call is in flight.
//
// Semantics:
//
//   - Short-circuit - if ctx is already canceled, the call is not started and ctx.Err() is returned.
//
//   - While fn is executing, and after ctx is canceled, we repeatedly invoke
//     duckdb_interrupt(conn) until fn returns, for cases when the interruptions are cleared internally in DuckDB.
//
//   - The interrupt loop is strictly scoped to the lifetime of this call and
//     stops immediately when fn returns, to avoid goroutine leaks.
//
//   - We never call interrupt unless ctx is canceled.
//
//   - The callback fn receives a derived context that carries an internal marker.
//     Any nested calls of runWithCtxInterrupt must pass that context forward
//     so the marker can suppress additional interrupter goroutines.
func runWithCtxInterrupt(ctx context.Context, conn mapping.Connection, fn func(context.Context) error) error {
	// Short circuit: do not start the DuckDB call if context is already canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	// guard: if an interrupter is already active for this call, don’t spawn another.
	// We tag the context to indicate an active interrupter scope.
	// The scope belongs to its connection: a call on another connection, such as a query
	// that a UDF runs with its context, starts its own interrupter.
	if scope, ok := ctx.Value(interruptScopeKey{}).(*interruptScope); ok && scope.conn == conn.Ptr {
		return fn(ctx)
	}
	scope := &interruptScope{conn: conn.Ptr}
	ctx = context.WithValue(ctx, interruptScopeKey{}, scope)

	bgDoneCh := make(chan struct{})
	done := make(chan struct{})

	go interrupterRoutine(ctx, conn, done, bgDoneCh)

	var once sync.Once
	scope.stop = func() {
		once.Do(func() {
			close(done)

			// Wait for interrupter goroutine to finish
			// Sometimes the go-routine is not scheduled immediately.
			// By the time it is scheduled, another query might be running on this connection.
			// If we don't wait for the go-routine to finish, it can cancel that new query.
			<-bgDoneCh
		})
	}

	// Stop the interrupter in a deferred call, so that it also stops if fn panics.
	// Callers may recover the panic and keep using the connection. A leftover
	// interrupter would then interrupt every later query on this connection once ctx
	// is canceled, and call Interrupt on a freed connection after it is closed.
	// A streaming result takes over the interrupter instead, see keepCtxInterrupt.
	defer func() {
		if !scope.kept {
			scope.stop()
		}
	}()

	// We pass `ctx` to keep that "enriched" context with the mark that we've already spawned a go-routine
	return fn(ctx)
}

type interruptScopeKey struct{}

// interruptScope is the interrupter of the outermost runWithCtxInterrupt call.
type interruptScope struct {
	// conn is the connection that the interrupter interrupts.
	conn unsafe.Pointer
	// stop stops the interrupter and waits for it to finish. It is safe to call more than once.
	stop func()
	// kept is true once a streaming result has taken over the interrupter.
	kept bool
}

// keepCtxInterrupt keeps the interrupter of the runWithCtxInterrupt call that ctx belongs to
// running after that call returns, and returns the function that stops it.
// A streaming result runs its query while it is fetched, so cancellation must reach DuckDB
// until the result is closed. ctx must be the context passed to fn by runWithCtxInterrupt
// for conn.
func keepCtxInterrupt(ctx context.Context, conn mapping.Connection) func() {
	scope, ok := ctx.Value(interruptScopeKey{}).(*interruptScope)
	if !ok || scope.conn != conn.Ptr {
		return func() {}
	}
	scope.kept = true
	return scope.stop
}

func interrupterRoutine(ctx context.Context, conn mapping.Connection, done <-chan struct{}, bgDoneCh chan<- struct{}) {
	defer close(bgDoneCh)

	select {
	case <-ctx.Done():
	case <-done:
		// finished before cancellation
		return
	}

	// Re-assert interruption until the wrapped function finishes.
	ticker := time.NewTicker(interruptInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		default:
			mapping.Interrupt(conn)
		}

		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}
