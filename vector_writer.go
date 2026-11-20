package duckdb

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriter writes a DuckDB vector. It is valid as long as the
// underlying vector is valid and writable.
type VectorWriter[T vectorValue] struct {
	vector Vector
}

// GetVectorWriter returns a typed writer for a writable DuckDB vector. The
// caller must get the vector from a writable source.
// T must be string (including named string types), HugeInt, or UHugeInt,
// matching VARCHAR, HUGEINT, or UHUGEINT respectively.
func GetVectorWriter[T vectorValue](vector Vector) (VectorWriter[T], error) {
	writer, err := newVectorWriter[T](vector)
	if err != nil {
		return VectorWriter[T]{}, getError(errAPI, err)
	}
	return writer, nil
}

func newVectorWriter[T vectorValue](vector Vector) (VectorWriter[T], error) {
	if vector.v == nil {
		return VectorWriter[T]{}, errUninitializedVectorWriter
	}
	if vector.v.Type != vectorValueType[T]() || vector.v.isJSON {
		return VectorWriter[T]{}, vectorWriterTypeError[T](vector.v)
	}
	return VectorWriter[T]{vector: vector}, nil
}

// Len returns the vector's logical row count.
func (writer VectorWriter[T]) Len() int {
	return writer.vector.logicalCount
}

// Set writes value at row. DuckDB copies the value. For strings, Set returns
// an error if value is not valid UTF-8 and leaves the row unchanged.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
	switch v := any(value).(type) {
	case HugeInt:
		setPrimitive(writer.vector.v, rowIdx, mapping.NewHugeInt(v.Lower, v.Upper))
	case UHugeInt:
		setPrimitive(writer.vector.v, rowIdx, mapping.NewUHugeInt(v.Lower, v.Upper))
	default:
		// The constraint leaves only string and named string types here.
		stringValue := value
		s := *(*string)(unsafe.Pointer(&stringValue))
		b := unsafe.Slice(unsafe.StringData(s), len(s))
		if !validUTF8(b) {
			return getError(errAPI, errInvalidUTF8)
		}
		mapping.UnsafeVectorAssignStringElementLen(writer.vector.v.vec, rowIdx, b)
	}
	writer.vector.v.clearNull(rowIdx)
	return nil
}

// SetNull writes SQL NULL at row.
func (writer VectorWriter[T]) SetNull(row int) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}
	writer.vector.v.setNull(mapping.IdxT(row))
	return nil
}

func (writer VectorWriter[T]) checkRow(row int) error {
	if writer.vector.v == nil {
		return errUninitializedVectorWriter
	}
	if row < 0 || row >= writer.Len() {
		return rowIndexError(row, writer.Len())
	}
	return nil
}

func vectorWriterTypeError[T vectorValue](vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector writer type mismatch: DuckDB %s cannot be written as Go %s", actual, reflect.TypeFor[T]())
}
