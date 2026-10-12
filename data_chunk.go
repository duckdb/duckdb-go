package duckdb

import "C"

import (
	"errors"
	"sync"

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
}

// GetDataChunkCapacity returns the capacity of a data chunk.
var GetDataChunkCapacity = sync.OnceValue(func() int {
	return int(mapping.VectorSize())
})

// GetSize returns the internal size of the data chunk.
func (chunk *DataChunk) GetSize() int {
	chunk.size = int(mapping.DataChunkGetSize(chunk.chunk))
	return chunk.size
}

// ColumnCount returns the number of columns in the data chunk.
func (chunk *DataChunk) ColumnCount() int {
	return len(chunk.columns)
}

// GetVector returns a borrowed vector for a column. It is valid as long as the
// data chunk is valid.
func (chunk *DataChunk) GetVector(colIdx int) (Vector, error) {
	if chunk == nil {
		return Vector{}, getError(errAPI, errNilDataChunk)
	}

	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil {
		return Vector{}, getError(errAPI, err)
	}

	return newVector(&chunk.columns[colIdx], chunk.GetSize()), nil
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	mapping.DataChunkSetSize(chunk.chunk, mapping.IdxT(size))
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(colIdx, rowIdx int) (any, error) {
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
// Note that this requires casting the type for each invocation, which usually
// allocates. Prefer SetChunkValue on hot paths.
// If the column is not projected, the value is ignored.
// See SetChunkValue for how values map onto JSON-typed columns.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx, rowIdx int, val any) error {
	return setChunkVal(chunk, colIdx, rowIdx, val)
}

// SetChunkValue writes a single value to a column in a data chunk.
// Unlike DataChunk.SetValue, it keeps the caller's type rather than casting to
// any, so writes do not allocate unless the conversion itself has to.
// If the column is not projected, the value is ignored.
// Values written to JSON-typed columns are marshaled with encoding/json, so a
// string becomes a JSON string and a []byte becomes a base64-encoded JSON
// string. Use json.RawMessage to write a pre-serialized JSON document.
// NOTE: Custom ENUM types must be passed as string.
func SetChunkValue[T any](chunk DataChunk, colIdx, rowIdx int, val T) error {
	return setChunkVal(&chunk, colIdx, rowIdx, val)
}

// setChunkVal is the shared body of the three write entry points. It takes the
// chunk by pointer so that none of them copies it per value written.
func setChunkVal[T any](chunk *DataChunk, colIdx, rowIdx int, val T) error {
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil && errors.Is(err, errUnprojectedColumn) {
		return nil
	} else if err != nil {
		return getError(errAPI, err)
	}

	if err = setVectorVal(&chunk.columns[colIdx], mapping.IdxT(rowIdx), val); err != nil {
		return setValueError(colIdx, rowIdx, err)
	}

	return nil
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
	// NOTE: duckdb_create_data_chunk allocates each vector with DuckDB's standard
	// capacity, the current C API does not accept a smaller chunk capacity.
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
	chunk.GetSize()

	return err
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

	return nil
}

func (chunk *DataChunk) close() {
	mapping.DestroyDataChunk(&chunk.chunk)
}
