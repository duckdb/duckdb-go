package duckdb

import "C"

import (
	"errors"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// DataChunk storage of a DuckDB table.
type DataChunk struct {
	// data holds the underlying duckdb data chunk.
	chunk mapping.DataChunk
	// columns is a helper slice providing direct access to all columns.
	columns []vector
	// columnNames holds the column names, if known.
	columnNames []string
	// size caches the size after initialization.
	size int
	// projection mapping of projected columns, when known (otherwise empty)
	projection []int
	// closed is true after close released the underlying C-allocated memory.
	closed bool
}

// checkValid guards against reading or writing a data chunk whose C-allocated
// memory has already been freed.
//
// NOTE: This only covers the chunks that this package owns and closes itself.
// Chunks passed to scalar and table UDF callbacks are owned by DuckDB, which
// frees them once the callback returns: retaining one past its callback stays
// unsafe and undetectable here.
func (chunk *DataChunk) checkValid() error {
	if chunk.closed {
		return errClosedChunk
	}
	return nil
}

// GetDataChunkCapacity returns the capacity of a data chunk.
func GetDataChunkCapacity() int {
	return int(mapping.VectorSize())
}

// GetSize returns the internal size of the data chunk.
func (chunk *DataChunk) GetSize() int {
	if chunk.closed {
		return 0
	}
	chunk.size = int(mapping.DataChunkGetSize(chunk.chunk))
	return chunk.size
}

// ColumnCount returns the number of columns in the data chunk.
func (chunk *DataChunk) ColumnCount() int {
	return len(chunk.columns)
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if err := chunk.checkValid(); err != nil {
		return getError(errAPI, err)
	}
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	mapping.DataChunkSetSize(chunk.chunk, mapping.IdxT(size))
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(colIdx, rowIdx int) (any, error) {
	if err := chunk.checkValid(); err != nil {
		return nil, getError(errAPI, err)
	}
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil {
		return nil, getError(errAPI, err)
	}

	column := &chunk.columns[colIdx]
	value, err := column.getFn(column, mapping.IdxT(rowIdx))
	if err != nil {
		return nil, getError(errAPI, addIndexToError(err, colIdx))
	}
	return value, nil
}

// SetValue writes a single value to a column in a data chunk.
// Note that this requires casting the type for each invocation.
// If the column is not projected, the value is ignored.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx, rowIdx int, val any) error {
	if err := chunk.checkValid(); err != nil {
		return getError(errAPI, err)
	}
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil && errors.Is(err, errUnprojectedColumn) {
		return nil
	} else if err != nil {
		return getError(errAPI, err)
	}

	column := &chunk.columns[colIdx]
	if err = column.SetValue(rowIdx, val); err != nil {
		return setValueError(colIdx, rowIdx, val, err)
	}

	return nil
}

// SetChunkValue writes a single value to a column in a data chunk.
// The difference with `chunk.SetValue` is that `SetChunkValue` does not
// require casting the value to `any` (implicitly).
// If the column is not projected, the value is ignored.
// NOTE: Custom ENUM types must be passed as string.
func SetChunkValue[T any](chunk DataChunk, colIdx, rowIdx int, val T) error {
	if err := chunk.checkValid(); err != nil {
		return getError(errAPI, err)
	}
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil && errors.Is(err, errUnprojectedColumn) {
		return nil
	} else if err != nil {
		return getError(errAPI, err)
	}

	return setVectorVal(&chunk.columns[colIdx], mapping.IdxT(rowIdx), val)
}

func inBounds[T any](s []T, idx int) bool {
	return idx >= 0 && idx < len(s)
}

// verifyColIdx checks whether the provided column index is valid.
func (chunk *DataChunk) verifyAndRewriteColIdx(colIdx int) (int, error) {
	if chunk.projection == nil && (colIdx < 0 || colIdx >= len(chunk.columns)) {
		return colIdx, columnCountError(colIdx, len(chunk.columns))
	}

	if chunk.projection != nil && (colIdx < 0 || colIdx >= len(chunk.projection)) {
		return colIdx, columnCountError(colIdx, len(chunk.projection))
	}

	if chunk.projection != nil {
		origColIdx := colIdx
		colIdx = chunk.projection[colIdx]
		if !inBounds(chunk.columns, colIdx) {
			return colIdx, newUnprojectedColumnError(origColIdx)
		}
	}

	return colIdx, nil
}

func (chunk *DataChunk) initFromTypes(types []mapping.LogicalType, writable bool) error {
	// NOTE: initFromTypes does not initialize the column names.
	columnCount := len(types)

	// Initialize the callback functions to read and write values.
	chunk.columns = make([]vector, columnCount)
	var err error
	for i := range columnCount {
		if err = chunk.columns[i].init(types[i], i); err != nil {
			break
		}
	}
	if err != nil {
		return err
	}

	chunk.chunk = mapping.CreateDataChunk(types)
	chunk.initVectors(writable)
	chunk.closed = false

	return nil
}

func (chunk *DataChunk) reset(writable bool) {
	mapping.DataChunkReset(chunk.chunk)
	chunk.initVectors(writable)
}

func (chunk *DataChunk) initVectors(writable bool) {
	mapping.DataChunkSetSize(chunk.chunk, mapping.IdxT(GetDataChunkCapacity()))

	for i := range len(chunk.columns) {
		v := mapping.DataChunkGetVector(chunk.chunk, mapping.IdxT(i))
		chunk.columns[i].initVectors(v, writable)
	}
}

func (chunk *DataChunk) initFromDuckDataChunk(inputChunk mapping.DataChunk, writable bool) error {
	columnCount := mapping.DataChunkGetColumnCount(inputChunk)
	chunk.columns = make([]vector, columnCount)
	chunk.chunk = inputChunk
	chunk.closed = false

	var err error
	for i := range len(chunk.columns) {
		// Get the vector and initialize the callback functions to read and write values.
		vec := mapping.DataChunkGetVector(inputChunk, mapping.IdxT(i))
		logicalType := mapping.VectorGetColumnType(vec)
		err = chunk.columns[i].init(logicalType, i)
		mapping.DestroyLogicalType(&logicalType)
		if err != nil {
			break
		}

		// Initialize the vector and its child vectors.
		chunk.columns[i].initVectors(vec, writable)
	}
	if err != nil {
		return err
	}
	chunk.GetSize()

	return nil
}

func (chunk *DataChunk) initFromDuckVector(vec mapping.Vector, writable bool) error {
	columnCount := 1
	chunk.columns = make([]vector, columnCount)

	// Initialize the callback functions to read and write values.
	logicalType := mapping.VectorGetColumnType(vec)
	err := chunk.columns[0].init(logicalType, 0)
	mapping.DestroyLogicalType(&logicalType)
	if err != nil {
		return err
	}

	// Initialize the vector and its child vectors.
	chunk.columns[0].initVectors(vec, writable)
	chunk.closed = false

	return nil
}

// close releases the C-allocated memory of the data chunk.
func (chunk *DataChunk) close() {
	mapping.DestroyDataChunk(&chunk.chunk)
	chunk.closed = true
	chunk.columns = nil
	// NOTE: size stays as it was. rows.Next infers the end of a result from
	// rowCount == size, so zeroing it here makes Next skip its fetch loop and
	// return nil instead of io.EOF once the result is exhausted.
}
