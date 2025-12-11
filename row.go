package duckdb

import (
	"github.com/duckdb/duckdb-go/mapping"
)

// Row represents one row in duckdb.
// It references the internal vectors.
type Row struct {
	chunk *DataChunk
	r     mapping.IdxT
}

// IsProjected returns whether the column is projected.
func (r Row) IsProjected(colIdx int) bool {
	if len(r.chunk.projection) == 0 || colIdx < 0 || colIdx >= len(r.chunk.projection) {
		return false
	}

	return r.chunk.projection[colIdx] != -1
}

// SetRowValue sets the value at colIdx to val.
// Returns an error on failure, and nil for non-projected columns.
func SetRowValue[T any](row Row, colIdx int, val T) error {
	return SetChunkValue(*row.chunk, colIdx, int(row.r), val)
}

// SetRowValue sets the value at colIdx to val.
// Returns an error on failure, and nil for non-projected columns.
func (r Row) SetRowValue(colIdx int, val any) error {
	return SetRowValue(r, colIdx, val)
}
