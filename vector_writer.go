package duckdb

import (
	"fmt"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriter writes a DuckDB vector. It is valid as long as the
// underlying vector is valid and writable.
type VectorWriter[T vectorValue] struct {
	vector Vector
}

// GetVectorWriter returns a typed writer for a writable DuckDB vector. T must
// match the DuckDB type exactly. T can be bool, int8, int16, int32, int64,
// uint8, uint16, uint32, uint64, float32, float64, or string. T can also be a
// named Go type with one of these underlying types.
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
	if !vector.v.writable {
		return VectorWriter[T]{}, errVectorNotWritable
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

// Set writes value at row. For VARCHAR, DuckDB copies the value, and invalid
// UTF-8 can produce SQL NULL.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
	mapping.ValiditySetRowValidity(writer.vector.v.maskPtr, rowIdx, true)
	if writer.vector.v.Type == TYPE_VARCHAR {
		// T has an underlying string type because newVectorWriter validated it.
		stringValue := *(*string)(unsafe.Pointer(&value))
		mapping.VectorAssignStringElement(writer.vector.v.vec, rowIdx, stringValue)
		return nil
	}

	setPrimitive(writer.vector.v, rowIdx, value)
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
	return fmt.Errorf(
		"vector writer type mismatch: DuckDB %s cannot be written as Go %s",
		actual,
		vectorValueName[T](),
	)
}
