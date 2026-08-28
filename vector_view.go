package duckdb

import (
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// vectorValue is the set of Go values supported by vector access.
type vectorValue interface {
	~string
}

// VectorView gives read-only access to a DuckDB vector. It is valid as long as
// the underlying vector is valid.
type VectorView[T vectorValue] struct {
	vector Vector
}

// GetVectorView returns a typed read-only view of a DuckDB vector.
func GetVectorView[T vectorValue](vector Vector) (VectorView[T], error) {
	view, err := newVectorView[T](vector)
	if err != nil {
		return VectorView[T]{}, getError(errAPI, err)
	}
	return view, nil
}

// Len returns the vector's logical row count.
func (view VectorView[T]) Len() int {
	return view.vector.logicalCount
}

// GetValueBorrowed returns the value at row and whether it is non-NULL. The
// value is valid as long as the underlying vector is valid and unchanged.
func (view VectorView[T]) GetValueBorrowed(rowIdx int) (T, bool, error) {
	if err := view.checkRow(rowIdx); err != nil {
		var zero T
		return zero, false, getError(errAPI, err)
	}

	if view.vector.v.getNull(mapping.IdxT(rowIdx)) {
		var zero T
		return zero, false, nil
	}

	value := getBorrowedStringAt(view.vector.v.dataPtr, mapping.IdxT(rowIdx))
	return T(value), true, nil
}

func (view VectorView[T]) checkRow(rowIdx int) error {
	if view.vector.v == nil {
		return errUninitializedVectorView
	}
	if rowIdx < 0 || rowIdx >= view.Len() {
		return rowIndexError(rowIdx, view.Len())
	}
	return nil
}

func newVectorView[T vectorValue](vector Vector) (VectorView[T], error) {
	if vector.v == nil {
		return VectorView[T]{}, errUninitializedVectorView
	}
	if vector.v.Type != TYPE_VARCHAR || vector.v.isJSON {
		return VectorView[T]{}, vectorViewTypeError(vector.v)
	}
	return VectorView[T]{vector: vector}, nil
}

func vectorViewTypeError(vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector view type mismatch: DuckDB %s cannot be read as Go string", actual)
}
