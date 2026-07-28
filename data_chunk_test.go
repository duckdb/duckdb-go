package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDataChunkColumnNameUnknown covers the chunks that carry columns but no names.
func TestDataChunkColumnNameUnknown(t *testing.T) {
	chunk := DataChunk{columns: make([]vector, 2)}

	require.Nil(t, chunk.ColumnNames())
	_, err := chunk.ColumnName(0)
	require.ErrorIs(t, err, errUnknownColumnNames)

	// Out of range reports the index problem, not the missing names.
	_, err = chunk.ColumnName(2)
	require.ErrorContains(t, err, columnCountErrMsg)
}
