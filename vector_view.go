package duckdb

import (
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorValue is the set of Go values supported by vector access.
type VectorValue interface {
	~string
}

// VectorView gives read-only access to a DuckDB vector. It is valid as long as
// the underlying vector is valid.
type VectorView[T VectorValue] struct {
	v *vector

	// logicalCount is the number of rows visible through the view.
	// The view copies this count from the data chunk when it is created.
	logicalCount int
}

// GetVectorView returns a typed view over a data chunk column.
func GetVectorView[T VectorValue](chunk DataChunkView, column int) (VectorView[T], error) {
	dataChunk := chunk.dataChunk
	if dataChunk == nil {
		return VectorView[T]{}, getError(errAPI, errNilDataChunk)
	}

	column, err := dataChunk.verifyAndRewriteColIdx(column)
	if err != nil {
		return VectorView[T]{}, getError(errAPI, err)
	}

	view, err := newVectorView[T](&dataChunk.columns[column], dataChunk.GetSize())
	if err != nil {
		return VectorView[T]{}, getError(errAPI, err)
	}
	return view, nil
}

// Len returns the vector's logical row count.
func (view VectorView[T]) Len() int {
	return view.logicalCount
}

// GetValueBorrowed returns the value at row and whether it is non-NULL. The
// value is valid as long as the underlying vector is valid and unchanged.
func (view VectorView[T]) GetValueBorrowed(row int) (T, bool, error) {
	if err := view.checkRow(row); err != nil {
		var zero T
		return zero, false, getError(errAPI, err)
	}

	if view.v.getNull(mapping.IdxT(row)) {
		var zero T
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

func newVectorView[T VectorValue](vec *vector, size int) (VectorView[T], error) {
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
