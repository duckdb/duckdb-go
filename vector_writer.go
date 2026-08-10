package duckdb

import (
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriterValue is the set of Go values supported by VectorWriter.
type VectorWriterValue interface {
	VectorViewValue
}

// VectorWriter provides typed, mutable access to one supported DuckDB result
// vector. This first implementation supports only VectorWriter[string] for
// VARCHAR. The writer is callback-scoped and must not be retained after the
// ChunkContextExecutor returns.
//
// Set honors the scalar UDF's default NULL-in/NULL-out policy. Use
// SpecialNullHandling when the UDF must produce a non-NULL result for a row
// containing a NULL input.
type VectorWriter[T VectorWriterValue] struct {
	v            *vector
	logicalCount int
	iterState    *ChunkIteratorState
}

// GetResultVectorWriter returns a typed writer for the scalar UDF's result.
// This first implementation accepts only T=string and requires VARCHAR output.
func GetResultVectorWriter[T VectorWriterValue](
	iterState *ChunkIteratorState,
) (VectorWriter[T], error) {
	if iterState == nil || iterState.r.chunk == nil || iterState.output == nil {
		return VectorWriter[T]{}, getError(errAPI, errUninitializedChunkIterator)
	}
	if iterState.output.Type != TYPE_VARCHAR || iterState.output.isJSON {
		return VectorWriter[T]{}, getError(errAPI, vectorWriterTypeError(iterState.output))
	}

	// The input chunk defines the result row count. The current callback supplies
	// reserved flat result storage. A V2 backend must check its result size and
	// vector form before writing.
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

// Set writes a non-NULL VARCHAR at row. DuckDB's native assignment function
// ensures any out-of-line bytes are owned by DuckDB after the callback.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
	if writer.iterState.nullInNullOut && writer.iterState.inputRowIsNull(rowIdx) {
		writer.v.setNull(rowIdx)
		return nil
	}

	// The current C API changes invalid UTF-8 to SQL NULL. A V2 implementation
	// must preserve this behavior.
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
