package duckdb

import (
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorViewValue is the set of Go values supported by VectorView.
type VectorViewValue interface {
	~string
}

// VectorView gives read-only access to a DuckDB vector. It is valid only during
// the scalar UDF callback.
type VectorView[T VectorViewValue] struct {
	v            *vector
	logicalCount int
}

func getVectorView[T VectorViewValue](chunk *DataChunk, column int) (VectorView[T], error) {
	if chunk == nil {
		return VectorView[T]{}, getError(errAPI, errNilDataChunk)
	}

	column, err := chunk.verifyAndRewriteColIdx(column)
	if err != nil {
		return VectorView[T]{}, getError(errAPI, err)
	}

	view, err := newVectorView[T](&chunk.columns[column], chunk.GetSize())
	if err != nil {
		return VectorView[T]{}, getError(errAPI, err)
	}
	return view, nil
}

// GetInputVectorView returns a typed view over a scalar UDF input column.
func GetInputVectorView[T VectorViewValue](
	iterState *ChunkIteratorState,
	column int,
) (VectorView[T], error) {
	if iterState == nil || iterState.r.chunk == nil {
		return VectorView[T]{}, getError(errAPI, errUninitializedChunkIterator)
	}
	return getVectorView[T](iterState.r.chunk, column)
}

// Len returns the vector's logical row count.
func (view VectorView[T]) Len() int {
	return view.logicalCount
}

// GetValueBorrowed returns the value at row and whether it is non-NULL. The
// value is valid only during the scalar UDF callback.
func (view VectorView[T]) GetValueBorrowed(row int) (T, bool, error) {
	var zero T
	if err := view.checkRow(row); err != nil {
		return zero, false, getError(errAPI, err)
	}

	if view.v.getNull(mapping.IdxT(row)) {
		return zero, false, nil
	}

	value := getBorrowedStringAt(view.v.dataPtr, mapping.IdxT(row))
	return T(value), true, nil
}

func (view VectorView[T]) checkRow(row int) error {
	if view.v == nil {
		return errUninitializedVectorView
	}
	if row < 0 || row >= view.Len() {
		return rowIndexError(row, view.Len())
	}
	return nil
}

func newVectorView[T VectorViewValue](vec *vector, size int) (VectorView[T], error) {
	if vec.Type != TYPE_VARCHAR || vec.isJSON {
		return VectorView[T]{}, vectorViewTypeError(vec)
	}
	return VectorView[T]{v: vec, logicalCount: size}, nil
}

func vectorViewTypeError(vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector view type mismatch: DuckDB %s cannot be read as Go string", actual)
}

var (
	errNilDataChunk            = errors.New("nil data chunk")
	errUninitializedVectorView = errors.New("uninitialized vector view")
)
