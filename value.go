package duckdb

import (
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func createVarchar(s string) mapping.Value {
	return mapping.CreateVarcharLength(s, mapping.IdxT(len(s)))
}

func getValue(v mapping.Value) (any, error) {
	// NULL values keep their logical type, and DuckDB rejects reading them.
	if mapping.IsNullValue(v) {
		return nil, nil
	}

	// The logical type is valid as long as v (mapping.Value) is valid,
	// i.e., it must not be destroyed.
	lt := mapping.GetValueType(v)
	t := mapping.GetTypeId(lt)
	switch t {
	case TYPE_BOOLEAN:
		return mapping.GetBool(v), nil
	case TYPE_TINYINT:
		return mapping.GetInt8(v), nil
	case TYPE_SMALLINT:
		return mapping.GetInt16(v), nil
	case TYPE_INTEGER:
		return mapping.GetInt32(v), nil
	case TYPE_BIGINT:
		return mapping.GetInt64(v), nil
	case TYPE_UTINYINT:
		return mapping.GetUInt8(v), nil
	case TYPE_USMALLINT:
		return mapping.GetUInt16(v), nil
	case TYPE_UINTEGER:
		return mapping.GetUInt32(v), nil
	case TYPE_UBIGINT:
		return mapping.GetUInt64(v), nil
	case TYPE_FLOAT:
		return mapping.GetFloat(v), nil
	case TYPE_DOUBLE:
		return mapping.GetDouble(v), nil
	case TYPE_TIMESTAMP_S:
		ts := mapping.GetTimestampS(v)
		return getTSS(&ts), nil
	case TYPE_TIMESTAMP_MS:
		ts := mapping.GetTimestampMS(v)
		return getTSMS(&ts), nil
	case TYPE_TIMESTAMP_NS:
		ts := mapping.GetTimestampNS(v)
		return getTSNS(&ts), nil
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := mapping.GetTimestamp(v)
		return getTS(t, &ts), nil
	case TYPE_DATE:
		date := mapping.GetDate(v)
		return getDate(&date), nil
	case TYPE_TIME:
		ti := mapping.GetTime(v)
		return getTime(&ti), nil
	case TYPE_TIME_TZ:
		ti := mapping.GetTimeTZ(v)
		return getTimeTZ(&ti), nil
	case TYPE_INTERVAL:
		interval := mapping.GetInterval(v)
		return getInterval(&interval), nil
	case TYPE_BIT:
		bit := mapping.GetBit(v)
		defer mapping.DestroyBit(&bit)
		return Bit{Data: mapping.BitMembers(&bit)}, nil
	case TYPE_HUGEINT:
		hugeInt := mapping.GetHugeInt(v)
		return hugeIntToNative(&hugeInt), nil
	case TYPE_UHUGEINT:
		uhugeInt := mapping.GetUHugeInt(v)
		return uhugeIntToNative(&uhugeInt), nil
	case TYPE_BIGNUM:
		bigNum := mapping.GetBigNum(v)
		defer mapping.DestroyBigNum(&bigNum)
		return bigNumToNative(&bigNum), nil
	case TYPE_VARCHAR:
		return mapping.GetVarchar(v), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}

func createValue(lt mapping.LogicalType, val any) (mapping.Value, error) {
	// database/sql converts only top-level arguments.
	val, err := elementValue(val)
	if err != nil {
		return mapping.Value{}, err
	}
	if isNil(val) {
		return mapping.CreateNullValue(), nil
	}

	t := mapping.GetTypeId(lt)
	if isPrimitiveType(t) {
		val, _ = underlyingValue(val)
		if val, err = coerceElementValue(t, val); err != nil {
			return mapping.Value{}, err
		}
		return createPrimitiveValue(t, val)
	}

	switch t {
	case TYPE_ARRAY:
		return createSliceValue(lt, t, val)
	case TYPE_LIST:
		return createSliceValue(lt, t, val)
	case TYPE_STRUCT:
		return createStructValue(lt, val)
	case TYPE_MAP:
		return createMapValue(lt, val)
	default:
		return mapping.Value{}, unsupportedTypeError(reflect.TypeOf(val).Name())
	}
}

func checkCreatedValue(v mapping.Value) (mapping.Value, error) {
	// DuckDB returns a nil pointer when a value constructor rejects its input.
	if v.Ptr == nil {
		return mapping.Value{}, errCreateValue
	}
	return v, nil
}

func createPrimitiveValue(t mapping.Type, v any) (mapping.Value, error) {
	value, err := createPrimitiveValueRaw(t, v)
	if err != nil {
		return mapping.Value{}, err
	}
	return checkCreatedValue(value)
}

// createAs returns a cast error when v is not the Go type that create expects.
func createAs[T any](t mapping.Type, v any, create func(T) mapping.Value) (mapping.Value, error) {
	vv, ok := v.(T)
	if !ok {
		return mapping.Value{}, castErrorForValue(v, typeName(t))
	}
	return create(vv), nil
}

//nolint:gocyclo
func createPrimitiveValueRaw(t mapping.Type, v any) (mapping.Value, error) {
	switch t {
	case TYPE_SQLNULL:
		return mapping.CreateNullValue(), nil
	case TYPE_BOOLEAN:
		return createAs(t, v, mapping.CreateBool)
	case TYPE_TINYINT:
		return createAs(t, v, mapping.CreateInt8)
	case TYPE_SMALLINT:
		return createAs(t, v, mapping.CreateInt16)
	case TYPE_INTEGER:
		return createAs(t, v, mapping.CreateInt32)
	case TYPE_BIGINT:
		return createAs(t, v, mapping.CreateInt64)
	case TYPE_UTINYINT:
		return createAs(t, v, mapping.CreateUInt8)
	case TYPE_USMALLINT:
		return createAs(t, v, mapping.CreateUInt16)
	case TYPE_UINTEGER:
		return createAs(t, v, mapping.CreateUInt32)
	case TYPE_UBIGINT:
		return createAs(t, v, mapping.CreateUInt64)
	case TYPE_FLOAT:
		return createAs(t, v, mapping.CreateFloat)
	case TYPE_DOUBLE:
		return createAs(t, v, mapping.CreateDouble)
	case TYPE_VARCHAR:
		return createAs(t, v, createVarchar)
	case TYPE_TIMESTAMP:
		vv, err := inferTimestamp(t, v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestamp(vv), nil
	case TYPE_TIMESTAMP_TZ:
		vv, err := inferTimestamp(t, v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampTZ(vv), nil
	case TYPE_TIMESTAMP_S:
		vv, err := inferTimestampS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampS(vv), nil
	case TYPE_TIMESTAMP_MS:
		vv, err := inferTimestampMS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampMS(vv), nil
	case TYPE_TIMESTAMP_NS:
		vv, err := inferTimestampNS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampNS(vv), nil
	case TYPE_DATE:
		vv, err := inferDate(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateDate(vv), nil
	case TYPE_TIME:
		vv, err := inferTime(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTime(vv), nil
	case TYPE_TIME_TZ:
		vv, err := inferTimeTZ(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimeTZValue(vv), nil
	case TYPE_INTERVAL:
		vv, err := inferInterval(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateInterval(vv), nil
	case TYPE_HUGEINT:
		vv, err := inferHugeInt(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateHugeInt(vv), nil
	case TYPE_UHUGEINT:
		vv, err := inferUHugeInt(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateUHugeInt(vv), nil
	case TYPE_BIGNUM:
		vv, err := inferBigNum(v)
		if err != nil {
			return mapping.Value{}, err
		}
		defer mapping.DestroyBigNum(&vv)
		return mapping.CreateBigNum(vv), nil
	case TYPE_UUID:
		vv, err := inferUUID(v)
		if err != nil {
			return mapping.Value{}, err
		}
		lower, upper := mapping.HugeIntMembers(&vv)
		// CreateUUID expects unsigned UUID bits, not the sign-flipped internal hugeint.
		uHugeInt := mapping.NewUHugeInt(lower, uint64(upper)^(1<<63))
		return mapping.CreateUUID(uHugeInt), nil
	case TYPE_BIT:
		vv, ok := v.(Bit)
		if !ok {
			return mapping.Value{}, castErrorForValue(v, typeName(t))
		}
		bit := mapping.NewBit(vv.Data)
		defer mapping.DestroyBit(&bit)
		return mapping.CreateBit(bit), nil
	}
	return mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
}

func getPointerValue(v any) any {
	for {
		if v == nil {
			return nil
		}
		vo := reflect.ValueOf(v)
		if vo.Kind() == reflect.Pointer {
			if vo.IsNil() {
				return nil
			}
			// A *big.Int is a HUGEINT, UHUGEINT, or BIGNUM value rather than a pointer to one.
			if _, ok := v.(*big.Int); ok {
				return v
			}
			v = vo.Elem().Interface()
			continue
		}
		return v
	}
}

func needsNoConversion(v any) bool {
	switch v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, string, []byte, time.Time, []any, map[string]any,
		Interval, Decimal, UUID, Bit, Map, OrderedMap, Union:
		return true
	}
	return false
}

// valuerValue calls driver.Valuer like database/sql and returns nil for nil pointers. It keeps a
// *UUID, whose Value returns text, so that UUIDs are written in their binary form.
func valuerValue(v any) (any, error) {
	if needsNoConversion(v) {
		return v, nil
	}
	if id, ok := v.(*UUID); ok {
		if id == nil {
			return nil, nil
		}
		return v, nil
	}

	v, _, err := driverValue(v)
	if err != nil {
		return nil, err
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil, nil
	}
	return v, nil
}

func elementValue(v any) (any, error) {
	v, err := valuerValue(v)
	if err != nil || needsNoConversion(v) {
		return v, err
	}
	switch vv := v.(type) {
	case *big.Int:
		return v, nil
	case *UUID:
		return *vv, nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer {
		return elementValue(rv.Elem().Interface())
	}
	return v, nil
}

var underlyingTypes = map[reflect.Kind]reflect.Type{
	reflect.Bool:    reflectTypeBool,
	reflect.Int:     reflect.TypeFor[int](),
	reflect.Int8:    reflectTypeInt8,
	reflect.Int16:   reflectTypeInt16,
	reflect.Int32:   reflectTypeInt32,
	reflect.Int64:   reflectTypeInt64,
	reflect.Uint:    reflect.TypeFor[uint](),
	reflect.Uint8:   reflectTypeUint8,
	reflect.Uint16:  reflectTypeUint16,
	reflect.Uint32:  reflectTypeUint32,
	reflect.Uint64:  reflectTypeUint64,
	reflect.Float32: reflectTypeFloat32,
	reflect.Float64: reflectTypeFloat64,
	reflect.String:  reflectTypeString,
}

// underlyingValue converts a named type such as `type Status string` to its underlying type.
// Unlike database/sql, it keeps the size of numbers instead of widening them.
func underlyingValue(v any) (any, bool) {
	if needsNoConversion(v) {
		return v, false
	}
	rv := reflect.ValueOf(v)
	// Predeclared types and type literals have no package path.
	if rv.Type().PkgPath() == "" {
		return v, false
	}
	if t, ok := underlyingTypes[rv.Kind()]; ok {
		return rv.Convert(t).Interface(), true
	}
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return rv.Bytes(), true
	}
	return v, false
}

// coerceElementValue converts integers between sizes and float64 to FLOAT, as DuckDB casts
// top-level parameters, because Valuers such as sql.NullInt32 return int64.
func coerceElementValue(t Type, v any) (any, error) {
	// This also binds a []byte to VARCHAR as text.
	if inferred, vv := inferPrimitiveType(v); inferred == t {
		return vv, nil
	}
	switch t {
	case TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT,
		TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT, TYPE_FLOAT, TYPE_DOUBLE:
		return coerceTypedValue(t, v)
	}
	return v, nil
}

func isNil(i any) bool {
	if i == nil {
		return true
	}

	value := reflect.ValueOf(i)
	kind := value.Kind()

	switch kind {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Interface, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func inferLogicalTypeAndValue(v any) (mapping.LogicalType, mapping.Value, error) {
	// database/sql has not converted the elements of LIST and ARRAY values.
	v, err := valuerValue(v)
	if err != nil {
		return mapping.LogicalType{}, mapping.Value{}, err
	}

	// Try to create a primitive type.
	t, vv := inferPrimitiveType(v)
	if isPrimitiveType(t) {
		val, err := createPrimitiveValue(t, vv)
		if err != nil {
			return mapping.LogicalType{}, mapping.Value{}, err
		}
		return mapping.CreateLogicalType(t), val, err
	}

	if t == TYPE_MAP {
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
	}
	if t == TYPE_UNION {
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
	}

	// User-provided type with a Stringer interface:
	// We create a string and return a VARCHAR value.
	// TYPE_DECIMAL has a Stringer interface.
	if ss, ok := v.(fmt.Stringer); ok {
		t = TYPE_VARCHAR
		val, err := createPrimitiveValue(t, ss.String())
		if err != nil {
			return mapping.LogicalType{}, mapping.Value{}, err
		}
		return mapping.CreateLogicalType(t), val, err
	}

	// SQLNULL type.
	if isNil(v) {
		t = TYPE_SQLNULL
		val, err := createPrimitiveValue(t, v)
		if err != nil {
			return mapping.LogicalType{}, mapping.Value{}, err
		}
		return mapping.CreateLogicalType(t), val, err
	}

	// Named types such as `type Status string` infer their underlying type.
	if u, ok := underlyingValue(v); ok {
		return inferLogicalTypeAndValue(u)
	}

	// Complex types.
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Struct, reflect.Map:
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[TYPE_STRUCT])
	case reflect.Pointer:
		// Extract pointer and recurse.
		return inferLogicalTypeAndValue(getPointerValue(v))
	case reflect.Array, reflect.Slice:
		return inferSliceLogicalTypeAndValue(r.Interface(), r.Kind() == reflect.Array, r.Len())
	default:
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func inferPrimitiveType(v any) (Type, any) {
	// Return TYPE_INVALID for TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_ARRAY,
	// and for the unsupported types.
	t := TYPE_INVALID

	switch vv := v.(type) {
	case nil:
		t = TYPE_SQLNULL
	case bool:
		t = TYPE_BOOLEAN
	case int8:
		t = TYPE_TINYINT
	case int16:
		t = TYPE_SMALLINT
	case int32:
		t = TYPE_INTEGER
	case int64:
		t = TYPE_BIGINT
	case int:
		t = TYPE_BIGINT
		v = int64(vv)
	case uint8:
		t = TYPE_UTINYINT
	case uint16:
		t = TYPE_USMALLINT
	case uint32:
		t = TYPE_UINTEGER
	case uint64:
		t = TYPE_UBIGINT
	case uint:
		t = TYPE_UBIGINT
		v = uint64(vv)
	case float32:
		t = TYPE_FLOAT
	case float64:
		t = TYPE_DOUBLE
	case string:
		t = TYPE_VARCHAR
	case []byte:
		// No support for TYPE_BLOB.
		t = TYPE_VARCHAR
		v = string(vv)
	case mapping.Time:
		t = TYPE_TIME
	case time.Time:
		// Go's time.Time always carries timezone information (via Location),
		// so TIMESTAMP WITH TIME ZONE is the correct semantic mapping.
		t = TYPE_TIMESTAMP_TZ
	case Interval:
		t = TYPE_INTERVAL
	case *big.Int:
		t = TYPE_HUGEINT
	case Decimal:
		t = TYPE_DECIMAL
	case UUID:
		t = TYPE_UUID
	case Bit:
		t = TYPE_BIT
	case Map, OrderedMap:
		// We special-case TYPE_MAP to disambiguate with structs passed as map[string]any.
		t = TYPE_MAP
	case Union:
		t = TYPE_UNION
	}

	return t, v
}

func isPrimitiveType(t Type) bool {
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION:
		// Complex type.
		return false
	case TYPE_INVALID, TYPE_ANY, TYPE_VARIANT:
		// Invalid or unsupported.
		return false
	}
	return true
}

func inferSliceLogicalTypeAndValue[T any](val T, array bool, length int) (mapping.LogicalType, mapping.Value, error) {
	createFunc := mapping.CreateListValue
	typeFunc := mapping.CreateListType
	if array {
		createFunc = mapping.CreateArrayValue
		typeFunc = func(child mapping.LogicalType) mapping.LogicalType {
			return mapping.CreateArrayType(child, mapping.IdxT(length))
		}
	}

	slice, err := extractSlice(val)
	if err != nil {
		return mapping.LogicalType{}, mapping.Value{}, err
	}

	values := make([]mapping.Value, 0, length)
	defer func() { destroyValueSlice(values) }()

	logicalTypes := make([]mapping.LogicalType, 0, length)
	defer func() { destroyLogicalTypes(logicalTypes) }()

	var elemLogicalType mapping.LogicalType
	expectedIndex := -1
	expectedTypeStr := ""

	for i, v := range slice {
		et, vv, errInfer := inferLogicalTypeAndValue(v)
		if errInfer != nil {
			return mapping.LogicalType{}, mapping.Value{}, errInfer
		}
		values = append(values, vv)
		logicalTypes = append(logicalTypes, et)

		if et.Ptr != nil && mapping.GetTypeId(et) != TYPE_SQLNULL {
			if elemLogicalType.Ptr == nil {
				elemLogicalType = et
				expectedIndex = i
				expectedTypeStr = logicalTypeString(et)
				continue
			}
			// Check if this element's type matches the first non-null element's type
			if currentTypeStr := logicalTypeString(et); currentTypeStr != expectedTypeStr {
				return mapping.LogicalType{}, mapping.Value{},
					fmt.Errorf("mixed types in slice: cannot bind %s (index %d) and %s (index %d)",
						expectedTypeStr, expectedIndex, currentTypeStr, i)
			}
		}
	}

	if elemLogicalType.Ptr == nil {
		elemLogicalType = mapping.CreateLogicalType(TYPE_SQLNULL)
		defer mapping.DestroyLogicalType(&elemLogicalType)
	}
	v, err := checkCreatedValue(createFunc(elemLogicalType, values))
	if err != nil {
		return mapping.LogicalType{}, mapping.Value{}, err
	}
	return typeFunc(elemLogicalType), v, nil
}

func createSliceValue[T any](lt mapping.LogicalType, t Type, val T) (mapping.Value, error) {
	var childType mapping.LogicalType
	switch t {
	case TYPE_ARRAY:
		childType = mapping.ArrayTypeChildType(lt)
	case TYPE_LIST:
		childType = mapping.ListTypeChildType(lt)
	}
	defer mapping.DestroyLogicalType(&childType)

	slice, err := extractSlice(val)
	if err != nil {
		return mapping.Value{}, err
	}

	var values []mapping.Value
	defer func() { destroyValueSlice(values) }()

	for _, v := range slice {
		vv, err := createValue(childType, v)
		if err != nil {
			return mapping.Value{}, err
		}
		values = append(values, vv)
	}

	var v mapping.Value
	switch t {
	case TYPE_ARRAY:
		v = mapping.CreateArrayValue(childType, values)
	case TYPE_LIST:
		v = mapping.CreateListValue(childType, values)
	}

	return checkCreatedValue(v)
}

func createStructValue(lt mapping.LogicalType, val any) (mapping.Value, error) {
	m, ok := val.(map[string]any)
	if !ok {
		return mapping.Value{}, castError(reflect.TypeOf(val).Name(), reflectTypeMapString.Name())
	}

	var values []mapping.Value
	defer func() { destroyValueSlice(values) }()

	childCount := mapping.StructTypeChildCount(lt)
	for i := range uint64(childCount) {
		name := mapping.StructTypeChildName(lt, mapping.IdxT(i))
		t := mapping.StructTypeChildType(lt, mapping.IdxT(i))
		defer mapping.DestroyLogicalType(&t)

		// A missing field is nil, which binds as NULL.
		vv, err := createValue(t, m[name])
		if err != nil {
			return mapping.Value{}, err
		}
		values = append(values, vv)
	}

	return checkCreatedValue(mapping.CreateStructValue(lt, values))
}

func createMapValue(lt mapping.LogicalType, val any) (mapping.Value, error) {
	var m OrderedMap
	switch v := val.(type) {
	case OrderedMap:
		m = v
	case Map:
		m = orderedMapFromMap(v)
	default:
		return mapping.Value{}, castError(reflect.TypeOf(val).String(), reflectTypeMap.Name())
	}

	keyType := mapping.MapTypeKeyType(lt)
	defer mapping.DestroyLogicalType(&keyType)
	valueType := mapping.MapTypeValueType(lt)
	defer mapping.DestroyLogicalType(&valueType)

	keys := make([]mapping.Value, m.Len())
	defer destroyValueSlice(keys)
	for i, k := range m.keys {
		k, err := elementValue(k)
		if err != nil {
			return mapping.Value{}, err
		}
		if isNil(k) {
			return mapping.Value{}, errNullMapKey
		}
		kv, err := createValue(keyType, k)
		if err != nil {
			return mapping.Value{}, err
		}
		keys[i] = kv
	}

	values := make([]mapping.Value, m.Len())
	defer destroyValueSlice(values)
	for i, v := range m.values {
		vv, err := createValue(valueType, v)
		if err != nil {
			return mapping.Value{}, err
		}
		values[i] = vv
	}

	return checkCreatedValue(mapping.CreateMapValue(lt, keys, values))
}

func destroyValueSlice(values []mapping.Value) {
	for _, v := range values {
		mapping.DestroyValue(&v)
	}
}

func canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	default:
		return false
	}
}

func extractSlice[S any](val S) ([]any, error) {
	var s []any
	switch v := any(val).(type) {
	case []any:
		s = v
	default:
		kind := reflect.TypeOf(val).Kind()
		if kind != reflect.Array && kind != reflect.Slice {
			return nil, castError(reflect.TypeOf(val).String(), reflectTypeSliceAny.String())
		}

		// Insert the values into the slice.
		rv := reflect.ValueOf(val)
		s = make([]any, rv.Len())
		for i := range rv.Len() {
			idx := rv.Index(i)
			if canNil(idx) && idx.IsNil() {
				s[i] = nil
				continue
			}
			s[i] = idx.Interface()
		}
	}

	return s, nil
}
