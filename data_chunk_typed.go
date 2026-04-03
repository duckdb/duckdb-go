package duckdb

import (
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// IsNull reports whether the value at colIdx/rowIdx is NULL.
func (chunk *DataChunk) IsNull(colIdx, rowIdx int) (bool, error) {
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil {
		return false, getError(errAPI, err)
	}

	return chunk.columns[colIdx].getNull(mapping.IdxT(rowIdx)), nil
}

// Int32Slice returns a zero-copy view over an INTEGER column.
// The returned slice is only valid until the chunk is invalidated.
// For NULL rows, use IsNull to check whether the corresponding element is valid.
func (chunk *DataChunk) Int32Slice(colIdx int) ([]int32, error) {
	return typedChunkSlice[int32](chunk, colIdx, TYPE_INTEGER)
}

// Int64Slice returns a zero-copy view over a BIGINT column.
// The returned slice is only valid until the chunk is invalidated.
// For NULL rows, use IsNull to check whether the corresponding element is valid.
func (chunk *DataChunk) Int64Slice(colIdx int) ([]int64, error) {
	return typedChunkSlice[int64](chunk, colIdx, TYPE_BIGINT)
}

// Float64Slice returns a zero-copy view over a DOUBLE column.
// The returned slice is only valid until the chunk is invalidated.
// For NULL rows, use IsNull to check whether the corresponding element is valid.
func (chunk *DataChunk) Float64Slice(colIdx int) ([]float64, error) {
	return typedChunkSlice[float64](chunk, colIdx, TYPE_DOUBLE)
}

// TimestampMicrosSlice returns TIMESTAMP values as Unix microseconds.
// The returned slice is only valid until the chunk is invalidated.
// For NULL rows, use IsNull to check whether the corresponding element is valid.
func (chunk *DataChunk) TimestampMicrosSlice(colIdx int) ([]int64, error) {
	values, err := typedChunkSlice[mapping.Timestamp](chunk, colIdx, TYPE_TIMESTAMP)
	if err != nil {
		return nil, err
	}

	micros := make([]int64, len(values))
	for idx := range values {
		micros[idx] = mapping.TimestampMembers(&values[idx])
	}

	return micros, nil
}

func typedChunkSlice[T any](chunk *DataChunk, colIdx int, expected Type) ([]T, error) {
	colIdx, err := chunk.verifyAndRewriteColIdx(colIdx)
	if err != nil {
		return nil, getError(errAPI, err)
	}

	column := &chunk.columns[colIdx]
	if column.Type != expected {
		return nil, getError(errAPI, invalidInputError(typeToStringMap[column.Type], typeToStringMap[expected]))
	}

	size := chunk.GetSize()
	if size == 0 {
		return []T{}, nil
	}

	return unsafe.Slice((*T)(column.dataPtr), size), nil
}
