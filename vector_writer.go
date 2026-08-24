package duckdb

import (
	"fmt"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// VectorWriter writes a DuckDB vector. It is valid as long as the
// underlying vector is valid and writable.
type VectorWriter[T vectorValue] struct {
	vector Vector
}

// GetVectorWriter returns a typed writer for a writable DuckDB vector.
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
	if vector.v.Type != TYPE_VARCHAR || vector.v.isJSON {
		return VectorWriter[T]{}, vectorWriterTypeError(vector.v)
	}
	return VectorWriter[T]{vector: vector}, nil
}

// Len returns the vector's logical row count.
func (writer VectorWriter[T]) Len() int {
	return writer.vector.logicalCount
}

// Set writes value at row. DuckDB copies valid values. Invalid UTF-8 can
// produce SQL NULL.
func (writer VectorWriter[T]) Set(row int, value T) error {
	if err := writer.checkRow(row); err != nil {
		return getError(errAPI, err)
	}

	rowIdx := mapping.IdxT(row)
	mapping.ValiditySetRowValidity(writer.vector.v.maskPtr, rowIdx, true)
	mapping.VectorAssignStringElement(writer.vector.v.vec, rowIdx, string(value))
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

func vectorWriterTypeError(vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf("vector writer type mismatch: DuckDB %s cannot be written as Go string", actual)
}
