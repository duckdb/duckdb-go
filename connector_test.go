package duckdb

import (
	"testing"
)

func TestConnector_Close(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	// Multiple close calls must not cause panics or errors.
	closeConnectorWrapper(t, c)
	closeConnectorWrapper(t, c)
}
