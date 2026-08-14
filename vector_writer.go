package duckdb

import (
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriter writes a DuckDB result vector. It is valid as long as the
// underlying vector is valid and writable.
type VectorWriter[T VectorValue] struct {
	v            *vector
	logicalCount int
}

// GetResultVectorWriter returns a VARCHAR writer for a scalar UDF result.
func GetResultVectorWriter[T VectorValue](
	iterState *ChunkIteratorState,
) (VectorWriter[T], error) {
	if iterState == nil || iterState.r.chunk == nil || iterState.output == nil {
		return VectorWriter[T]{}, getError(errAPI, errUninitializedChunkIterator)
	}

	writer, err := newVectorWriter[T](iterState.output, iterState.r.chunk.GetSize())
	if err != nil {
		return VectorWriter[T]{}, getError(errAPI, err)
	}
	return writer, nil
}

func newVectorWriter[T VectorValue](vec *vector, logicalCount int) (VectorWriter[T], error) {
	if vec == nil {
		return VectorWriter[T]{}, errUninitializedVectorWriter
	}
	if vec.Type != TYPE_VARCHAR || vec.isJSON {
		return VectorWriter[T]{}, vectorWriterTypeError(vec)
	}
	return VectorWriter[T]{
		v:            vec,
		logicalCount: logicalCount,
	}, nil
}

// Len returns the result vector's logical row count.
func (writer VectorWriter[T]) Len() int {
	return writer.logicalCount
}

// Set writes value at row. DuckDB copies valid values. Invalid UTF-8 can
// produce SQL NULL.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
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
