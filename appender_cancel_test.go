package duckdb

import (
	"context"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

type appenderSpies struct {
	flush                bool
	clear                bool
	destroyAppenderCalls int
	destroyChunkCalls    int
	resetChunk           bool
	getChunkVector       bool
}

// stubAppenderMapping installs no-op/spy mapping hooks for appender cancellation tests.
func stubAppenderMapping(t *testing.T, flushState mapping.State, appenderErr string) *appenderSpies {
	t.Helper()

	spies := &appenderSpies{}
	patchVar(t, &mapping.Interrupt, func(_ mapping.Connection) {})

	// Spy hooks record the appender and chunk operations under test.
	patchVar(t, &mapping.AppenderFlush, func(_ mapping.Appender) mapping.State {
		spies.flush = true
		return flushState
	})
	patchVar(t, &mapping.AppenderError, func(_ mapping.Appender) string {
		return appenderErr
	})
	patchVar(t, &mapping.AppenderClear, func(_ mapping.Appender) mapping.State {
		spies.clear = true
		return mapping.StateSuccess
	})
	patchVar(t, &mapping.AppenderDestroy, func(_ *mapping.Appender) mapping.State {
		spies.destroyAppenderCalls++
		return mapping.StateSuccess
	})
	patchVar(t, &mapping.DestroyDataChunk, func(_ *mapping.DataChunk) {
		spies.destroyChunkCalls++
	})
	patchVar(t, &mapping.DataChunkReset, func(_ mapping.DataChunk) {
		spies.resetChunk = true
	})

	// Safety no-ops keep fake handles away from the C API if chunk reset is reached.
	patchVar(t, &mapping.DataChunkSetSize, func(_ mapping.DataChunk, _ mapping.IdxT) {})
	patchVar(t, &mapping.DataChunkGetVector, func(_ mapping.DataChunk, _ mapping.IdxT) mapping.Vector {
		spies.getChunkVector = true
		return mapping.Vector{}
	})
	patchVar(t, &mapping.VectorGetData, func(_ mapping.Vector) unsafe.Pointer {
		return nil
	})
	patchVar(t, &mapping.VectorEnsureValidityWritable, func(_ mapping.Vector) {})
	patchVar(t, &mapping.VectorGetValidity, func(_ mapping.Vector) unsafe.Pointer {
		return nil
	})
	return spies
}

func newStubAppender() *Appender {
	return &Appender{
		conn: &Conn{},
	}
}

func preCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// The tests below patch package-level mapping hooks and must not run in parallel.
func TestAppenderFlushWithCancelPreCanceledAttemptsFlush(t *testing.T) {
	// This characterizes the fast success path; failure and interrupt handling are covered below.
	spies := stubAppenderMapping(t, mapping.StateSuccess, "")

	ctx := preCanceledContext()
	a := newStubAppender()

	require.NoError(t, a.flushWithCancel(ctx))
	require.True(t, spies.flush, "AppenderFlush should run when ctx is pre-canceled")
}

func TestAppenderCloseWithCancelPreCanceledFlushesAndCleansUp(t *testing.T) {
	// This characterizes the fast success path; failure and interrupt handling are covered below.
	spies := stubAppenderMapping(t, mapping.StateSuccess, "")

	ctx := preCanceledContext()
	a := newStubAppender()

	require.NoError(t, a.CloseWithCancel(ctx))
	require.True(t, spies.flush, "AppenderFlush should run when ctx is pre-canceled")
	require.False(t, spies.clear, "successful pre-canceled close should not clear flushed appender state")
	require.Equal(t, 1, spies.destroyAppenderCalls, "pre-canceled close should destroy the appender")
	require.Equal(t, 1, spies.destroyChunkCalls, "pre-canceled close should destroy the data chunk")
}

func TestAppenderFlushWithCancelPreCanceledFailureIncludesContext(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateError, "Invalid Input Error: appender failed")

	ctx := preCanceledContext()
	a := newStubAppender()

	err := a.flushWithCancel(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "appender failed")

	err = a.FlushWithCancel(ctx)
	require.ErrorIs(t, err, errAppenderFlush)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "appender failed")
	require.True(t, spies.flush, "AppenderFlush should run when ctx is pre-canceled")
}

func TestAppenderCloseWithCancelFlushFailureClearsWithoutResettingChunk(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateError, "Invalid Input Error: appender failed")

	ctx := preCanceledContext()
	a := newStubAppender()
	a.chunk.columns = make([]vector, 1)

	err := a.CloseWithCancel(ctx)
	require.ErrorIs(t, err, errAppenderClose)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "appender failed")
	require.True(t, spies.clear, "failed close should clear buffered appender state")
	require.False(t, spies.resetChunk, "failed close should not reset the destroyed data chunk")
	require.False(t, spies.getChunkVector, "failed close should not reinitialize vectors on the destroyed data chunk")
}

func TestAppenderCloseWithCancelFlushAndClearFailuresAreJoined(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateError, "Invalid Input Error: flush failed")

	var appenderErrorCalls int
	patchVar(t, &mapping.AppenderError, func(_ mapping.Appender) string {
		appenderErrorCalls++
		if appenderErrorCalls == 1 {
			return "Invalid Input Error: flush failed"
		}
		return "Invalid Input Error: clear failed"
	})
	patchVar(t, &mapping.AppenderClear, func(_ mapping.Appender) mapping.State {
		spies.clear = true
		return mapping.StateError
	})

	a := newStubAppender()

	err := a.CloseWithCancel(context.Background())
	require.ErrorIs(t, err, errAppenderClose)
	require.ErrorContains(t, err, "flush failed")
	require.ErrorContains(t, err, "clear failed")
	require.ErrorContains(t, err, "failed to clear appender's internal data")
	require.True(t, spies.clear, "failed close should still try to clear buffered appender state")
}

func TestAppenderCloseWithCancelAppendFailureIncludesError(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateSuccess, "Invalid Input Error: append failed")

	var appendCalled bool
	patchVar(t, &mapping.AppendDataChunk, func(_ mapping.Appender, _ mapping.DataChunk) mapping.State {
		appendCalled = true
		return mapping.StateError
	})

	a := newStubAppender()
	a.rowCount = 1
	a.chunk.columns = make([]vector, 1)

	err := a.CloseWithCancel(context.Background())
	require.ErrorIs(t, err, errAppenderClose)
	require.ErrorContains(t, err, "append failed")
	require.True(t, appendCalled, "CloseWithCancel should append buffered rows before flushing")
	require.Equal(t, 1, spies.destroyAppenderCalls, "append failure should still destroy the appender")
	require.Equal(t, 1, spies.destroyChunkCalls, "append failure should still destroy the data chunk")
}

func TestAppenderCloseWithCancelDestroyFailureReturnsError(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateSuccess, "")
	patchVar(t, &mapping.AppenderDestroy, func(_ *mapping.Appender) mapping.State {
		spies.destroyAppenderCalls++
		return mapping.StateError
	})

	a := newStubAppender()

	err := a.CloseWithCancel(context.Background())
	require.ErrorIs(t, err, errAppenderClose)
	require.Equal(t, 1, spies.destroyAppenderCalls, "failed destroy should only run once")
	require.Equal(t, 1, spies.destroyChunkCalls, "failed destroy should still follow data chunk cleanup")
}

func TestAppenderCloseWithCancelDoubleCloseDoesNotDestroyAgain(t *testing.T) {
	spies := stubAppenderMapping(t, mapping.StateSuccess, "")

	a := newStubAppender()

	require.NoError(t, a.CloseWithCancel(context.Background()))

	err := a.CloseWithCancel(context.Background())
	require.ErrorIs(t, err, errAppenderDoubleClose)
	require.Equal(t, 1, spies.destroyAppenderCalls, "double close should not destroy the appender again")
	require.Equal(t, 1, spies.destroyChunkCalls, "double close should not destroy the data chunk again")
}
