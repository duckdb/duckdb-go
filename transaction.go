package duckdb

import (
	"context"
	"errors"
)

type tx struct {
	c *Conn
}

func (t *tx) Commit() error {
	if t.c == nil || !t.c.tx {
		panic("database/sql/driver: misuse of duckdb driver: extra Commit")
	}

	t.c.endStreamAtTxEnd(true)
	t.c.tx = false
	t.c.txCtx = nil
	txErr := t.c.txErr
	t.c.txErr = nil
	if txErr != nil {
		_, err := t.c.ExecContext(context.Background(), "ROLLBACK", nil)
		t.c = nil
		return errors.Join(errCommit, txErr, err)
	}

	_, err := t.c.ExecContext(context.Background(), "COMMIT TRANSACTION", nil)
	t.c = nil

	return err
}

func (t *tx) Rollback() error {
	if t.c == nil || !t.c.tx {
		panic("database/sql/driver: misuse of duckdb driver: extra Rollback")
	}

	t.c.endStreamAtTxEnd(false)
	t.c.tx = false
	t.c.txCtx = nil
	t.c.txErr = nil
	_, err := t.c.ExecContext(context.Background(), "ROLLBACK", nil)
	t.c = nil

	return err
}
