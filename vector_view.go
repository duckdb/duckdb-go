package duckdb

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// vectorValue is the set of Go values supported by typed vector access.
type vectorValue interface {
	~bool |
		~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		~string
}

func vectorValueType[T vectorValue]() Type {
	switch reflect.TypeFor[T]().Kind() {
	case reflect.Bool:
		return TYPE_BOOLEAN
	case reflect.Int8:
		return TYPE_TINYINT
	case reflect.Int16:
		return TYPE_SMALLINT
	case reflect.Int32:
		return TYPE_INTEGER
	case reflect.Int64:
		return TYPE_BIGINT
	case reflect.Uint8:
		return TYPE_UTINYINT
	case reflect.Uint16:
		return TYPE_USMALLINT
	case reflect.Uint32:
		return TYPE_UINTEGER
	case reflect.Uint64:
		return TYPE_UBIGINT
	case reflect.Float32:
		return TYPE_FLOAT
	case reflect.Float64:
		return TYPE_DOUBLE
	case reflect.String:
		return TYPE_VARCHAR
	default:
		return TYPE_INVALID
	}
}

func vectorValueName[T vectorValue]() string {
	return reflect.TypeFor[T]().Kind().String()
}

// VectorView provides typed, read-only access to a DuckDB vector.
//
// A VectorView does not own the underlying vector or control its lifetime. It
// must not be used after the underlying vector becomes invalid.
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

// GetValue returns the value at row and whether it is non-NULL.
//
// For VARCHAR, the returned Go string references bytes owned by the underlying
// vector and must not be retained after the vector becomes invalid or changes.
func (view VectorView[T]) GetValue(rowIdx int) (T, bool, error) {
	if err := view.checkRow(rowIdx); err != nil {
		var zero T
		return zero, false, getError(errAPI, err)
	}

	if view.vector.v.getNull(mapping.IdxT(rowIdx)) {
		var zero T
		return zero, false, nil
	}

	if view.vector.v.Type == TYPE_VARCHAR {
		value := getBorrowedStringAt(view.vector.v.dataPtr, mapping.IdxT(rowIdx))
		// T has an underlying string type because newVectorView validated it.
		return *(*T)(unsafe.Pointer(&value)), true, nil
	}

	return getPrimitive[T](view.vector.v, mapping.IdxT(rowIdx)), true, nil
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
	return fmt.Errorf(
		"vector view type mismatch: DuckDB %s cannot be read as Go %s",
		actual,
		vectorValueName[T](),
	)
}
