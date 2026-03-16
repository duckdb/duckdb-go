package duckdb

import (
	"database/sql/driver"
	"iter"
)

// chunk is the interface for reading from and writing to a DataChunk
type chunk interface {
	GetSize() int
	ColumnCount() int
	GetValue(colIdx, rowIdx int) (any, error)
	SetValue(colIdx, rowIdx int, val any) error
}

// ScalarUDFChunk provides row-oriented access to UDF input/output
type ScalarUDFChunk struct {
	input         chunk
	output        chunk
	nullInNullOut bool
}

// ScalarUDFRow represents a single row in a ScalarUDFChunk.
// It provides access to input values and allows setting the result.
type ScalarUDFRow struct {
	chunk  *ScalarUDFChunk
	rowIdx int
	// Args contains the User Defined Function arguments for this row
	Args []driver.Value
}

// RowCount returns the number of rows in this chunk.
func (c *ScalarUDFChunk) RowCount() int {
	return c.input.GetSize()
}

// ColumnCount returns the number of input columns.
func (c *ScalarUDFChunk) ColumnCount() int {
	return c.input.ColumnCount()
}

// Rows returns an iterator over all rows in the chunk and an onFinish callback.
// The iterator yields *ScalarUDFRow; the callback must be invoked after iteration
// to report any error that occurred during iteration (e.g. from reading input).
//
// When nullInNullOut is enabled (the default), rows containing any NULL input
// are automatically skipped and their result is set to NULL. This means your
// UDF only sees rows with all non-NULL inputs.
//
// Note: row.Args is reused across iterations for efficiency. Do not store
// references to Args beyond the current iteration - copy values if needed.
//
// Example:
//
//	rows, onFinish := chunk.Rows()
//	for row := range rows {
//	    result := row.Args[0].(int32) + row.Args[1].(int32)
//	    if err := row.SetResult(result); err != nil {
//	        return err
//	    }
//	}
//	return onFinish()
func (c *ScalarUDFChunk) Rows() (iter.Seq[*ScalarUDFRow], func() error) {
	var iterErr error
	seq := func(yield func(*ScalarUDFRow) bool) {
		colCount := c.ColumnCount()
		// Reuse args slice across iterations to reduce allocations.
		args := make([]driver.Value, colCount)

		for rowIdx := range c.RowCount() {
			hasNull := false

			for colIdx := range colCount {
				val, err := c.input.GetValue(colIdx, rowIdx)
				if err != nil {
					iterErr = err
					return
				}
				args[colIdx] = val

				if val == nil {
					hasNull = true
				}
			}

			// If nullInNullOut and row has NULL, auto-set result to NULL and skip
			if c.nullInNullOut && hasNull {
				iterErr = c.output.SetValue(0, rowIdx, nil)
				if iterErr != nil {
					// if we could not set a return value, stop iterating and surface the error
					return
				}
				continue
			}

			row := &ScalarUDFRow{
				chunk:  c,
				rowIdx: rowIdx,
				Args:   args,
			}

			wantNext := yield(row)
			if !wantNext {
				return
			}
		}
	}
	onFinish := func() error {
		return iterErr
	}
	return seq, onFinish
}

// SetResult writes the result value for this row directly to the output vector.
func (r *ScalarUDFRow) SetResult(val any) error {
	return r.chunk.output.SetValue(0, r.rowIdx, val)
}

// Index returns the row index within the chunk.
func (r *ScalarUDFRow) Index() int {
	return r.rowIdx
}
