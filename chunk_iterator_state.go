package duckdb

import (
	"database/sql/driver"
	"iter"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// ChunkIteratorState provides access to the input chunk and result vector of a
// ChunkContextExecutorFn. Rows supports row-by-row iteration.
type ChunkIteratorState struct {
	input         *DataChunk
	output        *vector
	currentRowIdx mapping.IdxT
	nullInNullOut bool
	args          []driver.Value
}

// SetResult sets the current row's output value.
// Call once per yielded row.
func (iterState *ChunkIteratorState) SetResult(val any) error {
	return iterState.output.SetValue(int(iterState.currentRowIdx), val)
}

// GetInputChunk returns the borrowed scalar UDF input chunk. Treat the chunk as
// read-only, and do not retain it after the scalar UDF callback returns.
func (iterState *ChunkIteratorState) GetInputChunk() *DataChunk {
	if iterState == nil {
		return nil
	}
	return iterState.input
}

// GetResultVector returns the borrowed writable scalar UDF result vector. Do
// not retain it after the scalar UDF callback returns.
func (iterState *ChunkIteratorState) GetResultVector() Vector {
	if iterState == nil || iterState.input == nil || iterState.output == nil {
		return Vector{}
	}
	return newVector(iterState.output, iterState.input.GetSize())
}

// GetValuePtr returns a pointer to the current row value for a column.
// Copy the value if you need to, as it is not retained between loop iterations.
func (iterState *ChunkIteratorState) GetValuePtr(colIdx int) *driver.Value {
	return &iterState.args[colIdx]
}

// ColumnCount returns the number of input columns of the iterated chunk.
func (iterState *ChunkIteratorState) ColumnCount() int {
	return len(iterState.args)
}

// Rows is used to iterate over the rows of a data chunk, and to set the result of a
// computation on a row in the output vector.
func (iterState *ChunkIteratorState) Rows() iter.Seq2[*ChunkIteratorState, error] {
	colCount := iterState.input.ColumnCount()

	return func(yield func(*ChunkIteratorState, error) bool) {
		var err error
		for rowIdx := range iterState.input.GetSize() {
			hasNull := false
			for colIdx := range colCount {
				// FIXME: Could likely be replaced with a vectorized getter function.
				iterState.args[colIdx], err = iterState.input.GetValue(colIdx, rowIdx)
				if err != nil {
					yield(nil, err)
					return
				}
				if iterState.args[colIdx] == nil {
					hasNull = true
					if iterState.nullInNullOut {
						break
					}
				}
			}

			if iterState.nullInNullOut && hasNull {
				// applyDefaultNullHandling sets the result to NULL after the callback.
				continue
			}

			iterState.currentRowIdx = mapping.IdxT(rowIdx)
			if !yield(iterState, nil) {
				return
			}
		}
	}
}
