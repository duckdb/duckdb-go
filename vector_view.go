package duckdb

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// vectorValue is the set of Go values supported by vector access. Strings
// are converted; every other type has the memory layout of its DuckDB
// storage and is copied as is.
type vectorValue interface {
	~string | HugeInt | UHugeInt
}

// Adding a fixed-width type to vectorValue needs its size check here.
var (
	_ [unsafe.Sizeof(mapping.HugeInt{})]byte  = [unsafe.Sizeof(HugeInt{})]byte{}
	_ [unsafe.Sizeof(mapping.UHugeInt{})]byte = [unsafe.Sizeof(UHugeInt{})]byte{}
)

func vectorValueType[T vectorValue]() Type {
	var zero T
	switch any(zero).(type) {
	case HugeInt:
		return TYPE_HUGEINT
	case UHugeInt:
		return TYPE_UHUGEINT
	default:
		return TYPE_VARCHAR
	}
}

// VectorView gives read-only access to a DuckDB vector. It is valid as long as
// the underlying vector is valid.
type VectorView[T vectorValue] struct {
	vector Vector
}

// GetVectorView returns a typed read-only view of a DuckDB vector.
// T must be string (including named string types), HugeInt, or UHugeInt,
// matching VARCHAR, HUGEINT, or UHUGEINT respectively.
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
// HugeInt and UHugeInt values are copied and remain valid independently of
// the vector. Reading these types does not allocate.
func (view VectorView[T]) GetValueBorrowed(rowIdx int) (T, bool, error) {
	if err := view.checkRow(rowIdx); err != nil {
		var zero T
		return zero, false, getError(errAPI, err)
	}

	vec, idx := view.vector.v, mapping.IdxT(rowIdx)
	if vec.getNull(idx) {
		var zero T
		return zero, false, nil
	}

	if vec.Type == TYPE_VARCHAR {
		// T is string or a named string type.
		s := getBorrowedStringAt(vec.dataPtr, idx)
		return *(*T)(unsafe.Pointer(&s)), true, nil
	}
	return getPrimitive[T](vec, idx), true, nil
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
	if vector.v.Type != vectorValueType[T]() || vector.v.isJSON {
		return VectorView[T]{}, vectorViewTypeError[T](vector.v)
	}
	return VectorView[T]{vector: vector}, nil
}

func vectorViewTypeError[T vectorValue](vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector view type mismatch: DuckDB %s cannot be read as Go %s", actual, reflect.TypeFor[T]())
}
