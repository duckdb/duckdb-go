package duckdb

import (
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriterValue is the set of Go values supported by VectorWriter.
type VectorWriterValue interface {
	~string
}

// VectorWriter writes a DuckDB result vector. It is valid only during the
// scalar UDF callback.
type VectorWriter[T VectorWriterValue] struct {
	v            *vector
	logicalCount int
	iterState    *ChunkIteratorState
}

// GetResultVectorWriter returns a VARCHAR writer for a scalar UDF result.
func GetResultVectorWriter[T VectorWriterValue](
	iterState *ChunkIteratorState,
) (VectorWriter[T], error) {
	if iterState == nil || iterState.r.chunk == nil || iterState.output == nil {
		return VectorWriter[T]{}, getError(errAPI, errUninitializedChunkIterator)
	}
	if iterState.output.Type != TYPE_VARCHAR || iterState.output.isJSON {
		return VectorWriter[T]{}, getError(errAPI, vectorWriterTypeError(iterState.output))
	}

	return VectorWriter[T]{
		v:            iterState.output,
		logicalCount: iterState.r.chunk.GetSize(),
		iterState:    iterState,
	}, nil
}

// Len returns the result vector's logical row count.
func (writer VectorWriter[T]) Len() int {
	return writer.logicalCount
}

// Set writes value at row. DuckDB copies valid values. Default NULL handling
// or invalid UTF-8 can produce SQL NULL.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
	if writer.iterState.nullInNullOut && writer.iterState.inputRowIsNull(rowIdx) {
		writer.v.setNull(rowIdx)
		return nil
	}

	mapping.ValiditySetRowValidity(writer.v.maskPtr, rowIdx, true)
	mapping.VectorAssignStringElement(writer.v.vec, rowIdx, string(value))
	return nil
}

// SetNull writes SQL NULL at row.
func (writer VectorWriter[T]) SetNull(row int) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}
	writer.v.setNull(mapping.IdxT(row))
	return nil
}

func (writer VectorWriter[T]) checkRow(row int) error {
	if writer.v == nil {
		return errUninitializedVectorWriter
	}
	if row < 0 || row >= writer.Len() {
		return rowIndexError(row, writer.Len())
	}
	return nil
}

func vectorWriterTypeError(vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector writer type mismatch: DuckDB %s cannot be written as Go string", actual)
}

var errUninitializedVectorWriter = errors.New("uninitialized vector writer")
