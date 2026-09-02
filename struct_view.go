package duckdb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

var (
	errUninitializedStructView = errors.New("uninitialized struct view")
	errEmptyStructFieldPath    = errors.New("STRUCT field path cannot be empty")
)

// StructView provides read-only access to the child vectors of a DuckDB
// STRUCT vector.
//
// A StructView does not own the underlying vector or control its lifetime. It
// must not be used after the underlying vector becomes invalid.
type StructView struct {
	vector Vector
}

// GetStructView returns a read-only view of a DuckDB STRUCT vector.
func GetStructView(vector Vector) (StructView, error) {
	if vector.v == nil {
		return StructView{}, getError(errAPI, errUninitializedStructView)
	}
	if vector.v.Type != TYPE_STRUCT {
		return StructView{}, getError(errAPI, structViewTypeError(vector.v))
	}
	return StructView{vector: vector}, nil
}

// Len returns the struct vector's logical row count.
func (view StructView) Len() int {
	return view.vector.logicalCount
}

// IsValid returns true if the struct value at row is non-NULL.
func (view StructView) IsValid(row int) (bool, error) {
	if err := view.checkRow(row); err != nil {
		return false, getError(errAPI, err)
	}
	return !view.vector.isNull(mapping.IdxT(row)), nil
}

// Field returns the borrowed vector at path. Multiple names descend through
// nested STRUCT fields, for example Field("invoice", "amount").
//
// The returned vector carries the validity of every enclosing STRUCT. A typed
// view of the field therefore reports NULL when any enclosing STRUCT is NULL.
func (view StructView) Field(path ...string) (Vector, error) {
	if view.vector.v == nil {
		return Vector{}, getError(errAPI, errUninitializedStructView)
	}
	if len(path) == 0 {
		return Vector{}, getError(errAPI, errEmptyStructFieldPath)
	}

	current := view.vector
	for depth, name := range path {
		if current.v.Type != TYPE_STRUCT {
			return Vector{}, getError(errAPI, fmt.Errorf(
				"cannot descend through %s: DuckDB type is %s, not STRUCT",
				strings.Join(path[:depth], "."),
				typeName(current.v.Type),
			))
		}

		childIndex := -1
		for i, entry := range current.v.structEntries {
			if entry.Name() == name {
				childIndex = i
				break
			}
		}
		if childIndex < 0 {
			return Vector{}, getError(errAPI, fmt.Errorf(
				"STRUCT field not found: %s",
				strings.Join(path[:depth+1], "."),
			))
		}

		current = current.structChild(&current.v.childVectors[childIndex])
	}
	return current, nil
}

func (view StructView) checkRow(row int) error {
	if view.vector.v == nil {
		return errUninitializedStructView
	}
	if row < 0 || row >= view.Len() {
		return rowIndexError(row, view.Len())
	}
	return nil
}

func structViewTypeError(vec *vector) error {
	return fmt.Errorf(
		"struct view type mismatch: DuckDB %s cannot be read as STRUCT",
		typeName(vec.Type),
	)
}
