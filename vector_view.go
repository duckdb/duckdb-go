package duckdb

import (
	"fmt"
	"reflect"
	"time"
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

// vectorViewValue is the set of logical scalar Go values supported by typed
// vector reads. Some values, such as DECIMAL and timestamps, do not have the
// same layout as DuckDB's physical vector storage and are converted on read.
type vectorViewValue interface {
	vectorValue |
		time.Time | Interval | UUID |
		Int128 | Uint128 | DecimalValue | TimeTZValue | RawJSON |
		~[]byte | Bit
}

type vectorViewKind uint8

const (
	vectorViewDirect vectorViewKind = iota
	vectorViewVarchar
	vectorViewEnum
	vectorViewTemporal
	vectorViewInterval
	vectorViewHugeInt
	vectorViewUHugeInt
	vectorViewDecimal
	vectorViewTimeTZ
	vectorViewUUID
	vectorViewBytes
	vectorViewBit
	vectorViewJSON
)

var (
	reflectTypeInt128      = reflect.TypeFor[Int128]()
	reflectTypeUint128     = reflect.TypeFor[Uint128]()
	reflectTypeDecimalView = reflect.TypeFor[DecimalValue]()
	reflectTypeTimeTZValue = reflect.TypeFor[TimeTZValue]()
	reflectTypeRawJSON     = reflect.TypeFor[RawJSON]()
)

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
type VectorView[T vectorViewValue] struct {
	vector Vector
	kind   vectorViewKind
}

// GetVectorView returns a typed read-only view of a DuckDB vector.
func GetVectorView[T vectorViewValue](vector Vector) (VectorView[T], error) {
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

// Get returns the value at row and true if it is non-NULL. If the value is
// NULL, it returns the zero value of T and false.
//
// Returned strings and byte slices can reference bytes owned by the underlying
// vector. They must not be retained after the vector becomes invalid or
// changes, and returned byte slices must not be modified.
func (view VectorView[T]) Get(rowIdx int) (T, bool, error) {
	if err := view.checkRow(rowIdx); err != nil {
		var zero T
		return zero, false, getError(errAPI, err)
	}

	if view.vector.isNull(mapping.IdxT(rowIdx)) {
		var zero T
		return zero, false, nil
	}

	switch view.kind {
	case vectorViewVarchar, vectorViewJSON:
		value := getBorrowedStringAt(view.vector.v.dataPtr, mapping.IdxT(rowIdx))
		if view.kind == vectorViewJSON {
			json := RawJSON{data: value}
			return castVectorViewValue[T](&json), true, nil
		}
		return castVectorViewValue[T](&value), true, nil
	case vectorViewEnum:
		value, err := view.vector.v.getEnum(mapping.IdxT(rowIdx))
		if err != nil {
			var zero T
			return zero, false, getError(errAPI, err)
		}
		return castVectorViewValue[T](&value), true, nil
	case vectorViewTemporal:
		value := getVectorViewTime(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewInterval:
		value := getVectorViewInterval(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewHugeInt:
		value := getVectorViewInt128(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewUHugeInt:
		value := getVectorViewUint128(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewDecimal:
		value := getVectorViewDecimal(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewTimeTZ:
		value := getVectorViewTimeTZ(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewUUID:
		value := getVectorViewUUID(view.vector.v, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewBytes:
		value := getBorrowedBytesAt(view.vector.v.dataPtr, mapping.IdxT(rowIdx))
		return castVectorViewValue[T](&value), true, nil
	case vectorViewBit:
		data := getBorrowedBytesAt(view.vector.v.dataPtr, mapping.IdxT(rowIdx))
		value := Bit{Data: data}
		return castVectorViewValue[T](&value), true, nil
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

func newVectorView[T vectorViewValue](vector Vector) (VectorView[T], error) {
	if vector.v == nil {
		return VectorView[T]{}, errUninitializedVectorView
	}
	kind, ok := getVectorViewKind[T](vector.v)
	if !ok {
		return VectorView[T]{}, vectorViewTypeError[T](vector.v)
	}
	return VectorView[T]{vector: vector, kind: kind}, nil
}

func vectorViewTypeError[T vectorViewValue](vec *vector) error {
	actual := typeName(vec.Type)
	if vec.isJSON {
		actual = aliasJSON
	}
	return fmt.Errorf(
		"vector view type mismatch: DuckDB %s cannot be read as Go %s",
		actual,
		vectorViewValueName[T](),
	)
}

func getVectorViewKind[T vectorViewValue](vec *vector) (vectorViewKind, bool) {
	t := reflect.TypeFor[T]()
	switch t {
	case reflectTypeTime:
		switch vec.Type {
		case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
			TYPE_TIMESTAMP_TZ, TYPE_DATE, TYPE_TIME:
			return vectorViewTemporal, true
		}
	case reflectTypeInterval:
		return vectorViewInterval, vec.Type == TYPE_INTERVAL
	case reflectTypeInt128:
		return vectorViewHugeInt, vec.Type == TYPE_HUGEINT
	case reflectTypeUint128:
		return vectorViewUHugeInt, vec.Type == TYPE_UHUGEINT
	case reflectTypeDecimalView:
		return vectorViewDecimal, vec.Type == TYPE_DECIMAL
	case reflectTypeTimeTZValue:
		return vectorViewTimeTZ, vec.Type == TYPE_TIME_TZ
	case reflectTypeUUID:
		return vectorViewUUID, vec.Type == TYPE_UUID
	case reflectTypeBit:
		return vectorViewBit, vec.Type == TYPE_BIT
	case reflectTypeRawJSON:
		return vectorViewJSON, vec.Type == TYPE_VARCHAR && vec.isJSON
	}

	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return vectorViewBytes, vec.Type == TYPE_BLOB || vec.Type == TYPE_GEOMETRY
	}
	if t.Kind() == reflect.String {
		if vec.Type == TYPE_ENUM {
			return vectorViewEnum, true
		}
		return vectorViewVarchar, vec.Type == TYPE_VARCHAR && !vec.isJSON
	}

	return vectorViewDirect, vec.Type == vectorValueTypeForKind(t.Kind())
}

func vectorValueTypeForKind(kind reflect.Kind) Type {
	switch kind {
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
	default:
		return TYPE_INVALID
	}
}

func vectorViewValueName[T vectorViewValue]() string {
	t := reflect.TypeFor[T]()
	switch t {
	case reflectTypeTime, reflectTypeInterval, reflectTypeInt128, reflectTypeUint128,
		reflectTypeDecimalView, reflectTypeTimeTZValue, reflectTypeUUID, reflectTypeBit, reflectTypeRawJSON:
		return t.String()
	default:
		return t.Kind().String()
	}
}

func castVectorViewValue[T vectorViewValue, S any](value *S) T {
	return *(*T)(unsafe.Pointer(value))
}

func getVectorViewTime(vec *vector, rowIdx mapping.IdxT) time.Time {
	switch vec.Type {
	case TYPE_DATE:
		days := getPrimitive[int32](vec, rowIdx)
		return time.Unix(int64(days)*secondsPerDay, 0).UTC()
	case TYPE_TIME:
		micros := getPrimitive[int64](vec, rowIdx)
		clock := time.UnixMicro(micros).UTC()
		return time.Date(1, time.January, 1, clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), time.UTC)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(getPrimitive[int64](vec, rowIdx)).UTC()
	case TYPE_TIMESTAMP_S:
		return time.Unix(getPrimitive[int64](vec, rowIdx), 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(getPrimitive[int64](vec, rowIdx)).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, getPrimitive[int64](vec, rowIdx)).UTC()
	}
	return time.Time{}
}

func getVectorViewInterval(vec *vector, rowIdx mapping.IdxT) Interval {
	type intervalData struct {
		months int32
		days   int32
		micros int64
	}
	value := getPrimitive[intervalData](vec, rowIdx)
	return Interval{Months: value.months, Days: value.days, Micros: value.micros}
}

func getVectorViewTimeTZ(vec *vector, rowIdx mapping.IdxT) TimeTZValue {
	type timeTZData struct {
		hour   int8
		minute int8
		second int8
		_      byte
		micros int32
		offset int32
	}
	raw := getPrimitive[mapping.TimeTZ](vec, rowIdx)
	value := mapping.FromTimeTZ(raw)
	data := *(*timeTZData)(unsafe.Pointer(&value))
	micros := (int64(data.hour)*60*60+int64(data.minute)*60+int64(data.second))*1_000_000 + int64(data.micros)
	return TimeTZValue{Micros: micros, OffsetSeconds: data.offset}
}

func getVectorViewInt128(vec *vector, rowIdx mapping.IdxT) Int128 {
	return getPrimitive[Int128](vec, rowIdx)
}

func getVectorViewUint128(vec *vector, rowIdx mapping.IdxT) Uint128 {
	return getPrimitive[Uint128](vec, rowIdx)
}

func getVectorViewDecimal(vec *vector, rowIdx mapping.IdxT) DecimalValue {
	var unscaled Int128
	switch vec.internalType {
	case TYPE_SMALLINT:
		value := int64(getPrimitive[int16](vec, rowIdx))
		unscaled = Int128{Lower: uint64(value), Upper: value >> 63}
	case TYPE_INTEGER:
		value := int64(getPrimitive[int32](vec, rowIdx))
		unscaled = Int128{Lower: uint64(value), Upper: value >> 63}
	case TYPE_BIGINT:
		value := getPrimitive[int64](vec, rowIdx)
		unscaled = Int128{Lower: uint64(value), Upper: value >> 63}
	case TYPE_HUGEINT:
		unscaled = getVectorViewInt128(vec, rowIdx)
	}
	return DecimalValue{Unscaled: unscaled, Width: vec.decimalWidth, Scale: vec.decimalScale}
}

func getVectorViewUUID(vec *vector, rowIdx mapping.IdxT) UUID {
	value := getPrimitive[Int128](vec, rowIdx)
	var uuid UUID
	for i := range 8 {
		uuid[i] = byte(uint64(value.Upper) >> (56 - 8*i))
		uuid[i+8] = byte(value.Lower >> (56 - 8*i))
	}
	uuid[0] ^= 1 << 7
	return uuid
}

func getBorrowedBytesAt(dataPtr unsafe.Pointer, rowIdx mapping.IdxT) []byte {
	value := getBorrowedStringAt(dataPtr, rowIdx)
	return unsafe.Slice(unsafe.StringData(value), len(value))
}
