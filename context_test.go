package duckdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Test that if the context is already canceled before entering the wrapper,
// the function is not executed and no interrupt is issued.
func TestRunWithCtxInterrupt_PreCanceled(t *testing.T) {
	// Stub Interrupt to count invocations.
	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) {
		count.Add(1)
	}
	t.Cleanup(func() { mapping.Interrupt = orig })

	// Pre-cancel the context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	var dummyConn mapping.Connection
	err := runWithCtxInterrupt(ctx, dummyConn, func(_ context.Context) error {
		called = true
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called, "fn should not be called when ctx is pre-canceled")
	require.Equal(t, int64(0), count.Load(), "Interrupt should not be called when ctx is pre-canceled")
}

// Test that when there is no cancellation, the wrapper does not call Interrupt.
func TestRunWithCtxInterrupt_NoCancel_NoInterrupt(t *testing.T) {
	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) { count.Add(1) }
	t.Cleanup(func() { mapping.Interrupt = orig })

	ctx := context.Background()
	called := false
	var dummyConn mapping.Connection
	err := runWithCtxInterrupt(ctx, dummyConn, func(_ context.Context) error {
		called = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, called, "fn should be called when ctx is not canceled")
	require.Equal(t, int64(0), count.Load(), "Interrupt should not be called without cancellation")
}

// Test that after cancellation, Interrupt is invoked repeatedly until fn returns.
func TestRunWithCtxInterrupt_Cancel_RepeatsUntilDone(t *testing.T) {
	// Use a shorter interval for testing.
	origInterval := interruptInterval
	interruptInterval = 100 * time.Millisecond
	t.Cleanup(func() { interruptInterval = origInterval })

	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) { count.Add(1) }
	t.Cleanup(func() { mapping.Interrupt = orig })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	allowReturn := make(chan struct{})

	var dummyConn mapping.Connection
	doneErrCh := make(chan error)
	go func() {
		err := runWithCtxInterrupt(ctx, dummyConn, func(_ context.Context) error {
			// Signal that fn is running.
			close(started)
			// Wait until test allows return, in the meantime, Interrupts should be happening.
			<-allowReturn
			return nil
		})
		doneErrCh <- err
	}()

	// Wait until the function has started, then cancel the context to trigger interrupts.
	<-started
	cancel()

	testTookTooLong := false
	// Wait until at least a few interrupts have been observed.
	deadline := time.Now().Add(interruptInterval * 5)
	for count.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
		// break if the test takes too long before we have enough interrupts
		if time.Now().After(deadline.Add(1 * time.Second)) {
			testTookTooLong = true
			break
		}
	}
	require.GreaterOrEqual(t, count.Load(), int64(1), "Interrupt should be called after cancellation")

	// Allow the function to finish
	// The wrapper should stop interrupting immediately after.
	close(allowReturn)
	err := <-doneErrCh
	require.NoError(t, err)
	require.False(t, testTookTooLong, "The test took too long without calling enough interruptions")

	// Validate that interruptions stop after the wrapped function finishes.
	finalCount := count.Load()
	// Wait longer than interruptInterval
	time.Sleep(interruptInterval + 100*time.Millisecond)
	require.Equal(t, finalCount, count.Load(), "Interrupt should not be called after fn returns")
}

// Test that recursively wrapping with the same context only spawns a single interrupter goroutine.
func TestRunWithCtxInterrupt_RecursiveOnlyOneGoroutine(t *testing.T) {
	// Set a high interval to ensure only one call per goroutine until deadline set below
	origInterval := interruptInterval
	interruptInterval = 1 * time.Second
	t.Cleanup(func() { interruptInterval = origInterval })

	// Stub Interrupt to count calls
	var interruptCount atomic.Int32
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) {
		interruptCount.Add(1)
	}
	t.Cleanup(func() { mapping.Interrupt = orig })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dummyConn mapping.Connection

	started := make(chan struct{})
	allowReturn := make(chan struct{})

	doneErrCh := make(chan error, 1)
	go func() {
		err := runWithCtxInterrupt(ctx, dummyConn, func(wctx context.Context) error {
			return runWithCtxInterrupt(wctx, dummyConn, func(wctx2 context.Context) error {
				return runWithCtxInterrupt(wctx2, dummyConn, func(_ context.Context) error {
					close(started)
					<-allowReturn
					return nil
				})
			})
		})
		doneErrCh <- err
	}()

	// Wait until innermost function is running, then cancel to exercise the loop.
	<-started
	cancel()

	// Give the scheduler a brief moment to start the outer interrupter goroutine.
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) && interruptCount.Load() == 0 {
		time.Sleep(200 * time.Microsecond)
	}

	// Only one interrupter goroutine should have been spawned despite nesting.
	require.Equal(t, int32(1), interruptCount.Load(), "only one interrupter goroutine should be spawned for recursive wrapping")

	// Finish the innermost work and ensure no error is returned.
	close(allowReturn)
	require.NoError(t, <-doneErrCh)
}
