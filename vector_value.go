package duckdb

import "reflect"

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
