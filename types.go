package duckdb

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/google/uuid"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// duckdb-go exports the following type wrappers:
// UUID, Map, Interval, Decimal, Union, Composite (optional, used to scan LIST and STRUCT).

// Pre-computed reflect type values to avoid repeated allocations.
var (
	reflectTypeBool      = reflect.TypeFor[bool]()
	reflectTypeInt8      = reflect.TypeFor[int8]()
	reflectTypeInt16     = reflect.TypeFor[int16]()
	reflectTypeInt32     = reflect.TypeFor[int32]()
	reflectTypeInt64     = reflect.TypeFor[int64]()
	reflectTypeUint8     = reflect.TypeFor[uint8]()
	reflectTypeUint16    = reflect.TypeFor[uint16]()
	reflectTypeUint32    = reflect.TypeFor[uint32]()
	reflectTypeUint64    = reflect.TypeFor[uint64]()
	reflectTypeFloat32   = reflect.TypeFor[float32]()
	reflectTypeFloat64   = reflect.TypeFor[float64]()
	reflectTypeTime      = reflect.TypeFor[time.Time]()
	reflectTypeInterval  = reflect.TypeFor[Interval]()
	reflectTypeBigInt    = reflect.TypeFor[*big.Int]()
	reflectTypeString    = reflect.TypeFor[string]()
	reflectTypeBytes     = reflect.TypeFor[[]byte]()
	reflectTypeDecimal   = reflect.TypeFor[Decimal]()
	reflectTypeSliceAny  = reflect.TypeFor[[]any]()
	reflectTypeMapString = reflect.TypeFor[map[string]any]()
	reflectTypeMap       = reflect.TypeFor[Map]()
	reflectTypeUnion     = reflect.TypeFor[Union]()
	reflectTypeAny       = reflect.TypeFor[any]()
	reflectTypeUUID      = reflect.TypeFor[UUID]()
)

type numericType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

const uuidLength = 16

type UUID [uuidLength]byte

// Scan implements the sql.Scanner interface.
func (u *UUID) Scan(v any) error {
	switch val := v.(type) {
	case []byte:
		if len(val) != uuidLength {
			return u.Scan(string(val))
		}
		copy(u[:], val)
	case string:
		id, err := uuid.Parse(val)
		if err != nil {
			return err
		}
		copy(u[:], id[:])
	default:
		return fmt.Errorf("invalid UUID value type: %T", val)
	}
	return nil
}

// String implements the fmt.Stringer interface.
func (u *UUID) String() string {
	buf := make([]byte, 36)

	hex.Encode(buf, u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])

	return string(buf)
}

// Value implements the driver.Valuer interface.
func (u *UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

func inferUUID(val any) (mapping.HugeInt, error) {
	var id UUID
	switch v := val.(type) {
	case UUID:
		id = v
	case *UUID:
		id = *v
	case []uint8:
		if len(v) != uuidLength {
			return mapping.HugeInt{}, castError(reflect.TypeOf(val).String(), reflectTypeUUID.String())
		}
		for i := range uuidLength {
			id[i] = v[i]
		}
	default:
		return mapping.HugeInt{}, castError(reflect.TypeOf(val).String(), reflectTypeUUID.String())
	}
	hi := uuidToHugeInt(id)
	return hi, nil
}

// duckdb_hugeint is composed of (lower, upper) components.
// The value is computed as: upper * 2^64 + lower

func hugeIntToUUID(hugeInt *mapping.HugeInt) []byte {
	// Flip the sign bit of the signed hugeint to transform it to UUID bytes.
	var val [uuidLength]byte
	lower, upper := mapping.HugeIntMembers(hugeInt)
	binary.BigEndian.PutUint64(val[:8], uint64(upper)^1<<63)
	binary.BigEndian.PutUint64(val[8:], lower)
	return val[:]
}

func uuidToHugeInt(uuid UUID) mapping.HugeInt {
	// Flip the sign bit.
	lower := binary.BigEndian.Uint64(uuid[8:])
	upper := binary.BigEndian.Uint64(uuid[:8])
	return mapping.NewHugeInt(lower, int64(upper^(1<<63)))
}

func hugeIntToNative(hugeInt *mapping.HugeInt) *big.Int {
	lower, upper := mapping.HugeIntMembers(hugeInt)
	i := big.NewInt(upper)
	i.Lsh(i, 64)
	i.Add(i, new(big.Int).SetUint64(lower))
	return i
}

func numToBigInt(val any) (*big.Int, error) {
	switch v := val.(type) {
	case uint8:
		return big.NewInt(int64(v)), nil
	case int8:
		return big.NewInt(int64(v)), nil
	case uint16:
		return big.NewInt(int64(v)), nil
	case int16:
		return big.NewInt(int64(v)), nil
	case uint32:
		return big.NewInt(int64(v)), nil
	case int32:
		return big.NewInt(int64(v)), nil
	case uint64:
		return new(big.Int).SetUint64(v), nil
	case int64:
		return big.NewInt(v), nil
	case uint:
		return new(big.Int).SetUint64(uint64(v)), nil
	case int:
		return big.NewInt(int64(v)), nil
	case float32:
		bigFloat := new(big.Float).SetFloat64(float64(v))
		bigInt, _ := bigFloat.Int(nil)
		return bigInt, nil
	case float64:
		bigFloat := new(big.Float).SetFloat64(v)
		bigInt, _ := bigFloat.Int(nil)
		return bigInt, nil
	case *big.Int:
		if v == nil {
			return nil, castError("nil *big.Int", "*big.Int")
		}
		return v, nil
	case Decimal:
		if v.Value == nil {
			return nil, castError("nil Decimal.Value", "*big.Int")
		}
		return v.Value, nil
	default:
		return nil, castError(reflect.TypeOf(val).String(), "*big.Int")
	}
}

func hugeIntFromNative(i *big.Int) (mapping.HugeInt, error) {
	d := big.NewInt(1)
	d.Lsh(d, 64)

	q := new(big.Int)
	r := new(big.Int)
	q.DivMod(i, d, r)

	if !q.IsInt64() {
		return mapping.HugeInt{}, fmt.Errorf("big.Int(%s) is too big for HUGEINT", i.String())
	}

	return mapping.NewHugeInt(r.Uint64(), q.Int64()), nil
}

func inferHugeInt(val any) (mapping.HugeInt, error) {
	var err error
	var hi mapping.HugeInt
	switch v := val.(type) {
	case uint8:
		hi = mapping.NewHugeInt(uint64(v), 0)
	case int8:
		hi = mapping.NewHugeInt(uint64(v), int64(v)>>63)
	case uint16:
		hi = mapping.NewHugeInt(uint64(v), 0)
	case int16:
		hi = mapping.NewHugeInt(uint64(v), int64(v)>>63)
	case uint32:
		hi = mapping.NewHugeInt(uint64(v), 0)
	case int32:
		hi = mapping.NewHugeInt(uint64(v), int64(v)>>63)
	case uint64:
		hi = mapping.NewHugeInt(v, 0)
	case int64:
		hi = mapping.NewHugeInt(uint64(v), v>>63)
	case uint:
		hi = mapping.NewHugeInt(uint64(v), 0)
	case int:
		hi = mapping.NewHugeInt(uint64(v), int64(v)>>63)
	default:
		var i *big.Int
		if i, err = numToBigInt(val); err != nil {
			return mapping.HugeInt{}, err
		}
		hi, err = hugeIntFromNative(i)
	}
	return hi, err
}

func uhugeIntToNative(uhi *mapping.UHugeInt) *big.Int {
	lower, upper := mapping.UHugeIntMembers(uhi)
	i := new(big.Int).SetUint64(upper)
	i.Lsh(i, 64)
	i.Add(i, new(big.Int).SetUint64(lower))
	return i
}

func uhugeIntFromNative(i *big.Int) (mapping.UHugeInt, error) {
	if i.Sign() < 0 {
		return mapping.UHugeInt{}, fmt.Errorf("big.Int(%s) is negative, cannot convert to UHUGEINT", i.String())
	}

	d := big.NewInt(1)
	d.Lsh(d, 64)

	q := new(big.Int)
	r := new(big.Int)
	q.DivMod(i, d, r)

	if !q.IsUint64() {
		return mapping.UHugeInt{}, fmt.Errorf("big.Int(%s) is too big for UHUGEINT", i.String())
	}

	return mapping.NewUHugeInt(r.Uint64(), q.Uint64()), nil
}

func bigNumToNative(bn *mapping.BigNum) *big.Int {
	data, isNegative := mapping.BigNumMembers(bn)

	// Data is in big-endian format, which is what big.Int.SetBytes expects.
	i := new(big.Int).SetBytes(data)
	if isNegative {
		i.Neg(i)
	}
	return i
}

func bigNumFromNative(i *big.Int) mapping.BigNum {
	isNegative := i.Sign() < 0

	// Get absolute value bytes in big-endian format (which is what NewBigNum expects).
	absVal := new(big.Int).Abs(i)
	bigEndian := absVal.Bytes()

	// Handle zero case - ensure at least one byte
	if len(bigEndian) == 0 {
		bigEndian = []byte{0}
	}

	return mapping.NewBigNum(bigEndian, isNegative)
}

func inferBigNum(val any) (mapping.BigNum, error) {
	i, err := numToBigInt(val)
	if err != nil {
		return mapping.BigNum{}, err
	}
	return bigNumFromNative(i), nil
}

func inferUHugeInt(val any) (mapping.UHugeInt, error) {
	var err error
	var uhi mapping.UHugeInt
	switch v := val.(type) {
	case uint8:
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case int8:
		if v < 0 {
			return mapping.UHugeInt{}, fmt.Errorf("negative value %d cannot be converted to UHUGEINT", v)
		}
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case uint16:
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case int16:
		if v < 0 {
			return mapping.UHugeInt{}, fmt.Errorf("negative value %d cannot be converted to UHUGEINT", v)
		}
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case uint32:
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case int32:
		if v < 0 {
			return mapping.UHugeInt{}, fmt.Errorf("negative value %d cannot be converted to UHUGEINT", v)
		}
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case uint64:
		uhi = mapping.NewUHugeInt(v, 0)
	case int64:
		if v < 0 {
			return mapping.UHugeInt{}, fmt.Errorf("negative value %d cannot be converted to UHUGEINT", v)
		}
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case uint:
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	case int:
		if v < 0 {
			return mapping.UHugeInt{}, fmt.Errorf("negative value %d cannot be converted to UHUGEINT", v)
		}
		uhi = mapping.NewUHugeInt(uint64(v), 0)
	default:
		var i *big.Int
		if i, err = numToBigInt(val); err != nil {
			return mapping.UHugeInt{}, err
		}
		uhi, err = uhugeIntFromNative(i)
	}
	return uhi, err
}

type Map map[any]any

func (m *Map) Scan(v any) error {
	data, ok := v.(Map)
	if !ok {
		return fmt.Errorf("invalid type `%T` for scanning `Map`, expected `Map`", data)
	}

	*m = data
	return nil
}

func mapKeysField() string {
	return "key"
}

func mapValuesField() string {
	return "value"
}

type Interval struct {
	Days   int32 `json:"days"`
	Months int32 `json:"months"`
	Micros int64 `json:"micros"`
}

func inferInterval(val any) (mapping.Interval, error) {
	var i Interval
	switch v := val.(type) {
	case Interval:
		i = v
	default:
		return mapping.Interval{}, castError(reflect.TypeOf(val).String(), reflectTypeInterval.String())
	}
	return mapping.NewInterval(i.Months, i.Days, i.Micros), nil
}

// Composite can be used as the `Scanner` type for any composite types (maps, lists, structs).
type Composite[T any] struct {
	t T
}

func (s Composite[T]) Get() T {
	return s.t
}

func (s *Composite[T]) Scan(v any) error {
	return mapstructure.Decode(v, &s.t)
}

const max_decimal_width = 38

type Decimal struct {
	Width uint8
	Scale uint8
	Value *big.Int
}

func (d Decimal) Float64() float64 {
	scale := big.NewInt(int64(d.Scale))
	factor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), scale, nil))
	value := new(big.Float).SetInt(d.Value)
	value.Quo(value, factor)
	f, _ := value.Float64()
	return f
}

func (d Decimal) String() string {
	// Get the sign, and return early, if zero.
	if d.Value.Sign() == 0 {
		return "0"
	}

	// Remove the sign from the string integer value
	var signStr string
	scaleless := d.Value.String()
	if d.Value.Sign() < 0 {
		signStr = "-"
		scaleless = scaleless[1:]
	}

	// Remove all zeros from the right side
	zeroTrimmed := strings.TrimRightFunc(scaleless, func(r rune) bool { return r == '0' })
	scale := int(d.Scale) - (len(scaleless) - len(zeroTrimmed))

	// If the string is still bigger than the scale factor, output it without a decimal point
	if scale <= 0 {
		return signStr + zeroTrimmed + strings.Repeat("0", -1*scale)
	}

	// Pad a number with 0.0's if needed
	if len(zeroTrimmed) <= scale {
		return fmt.Sprintf("%s0.%s%s", signStr, strings.Repeat("0", scale-len(zeroTrimmed)), zeroTrimmed)
	}
	return signStr + zeroTrimmed[:len(zeroTrimmed)-scale] + "." + zeroTrimmed[len(zeroTrimmed)-scale:]
}

type Union struct {
	Value driver.Value `json:"value"`
	Tag   string       `json:"tag"`
}

func castToTime(val any) (time.Time, error) {
	var ti time.Time
	switch v := val.(type) {
	case time.Time:
		ti = v
	default:
		return ti, castError(reflect.TypeOf(val).String(), reflectTypeTime.String())
	}
	return ti, nil
}

func getTSTicks(t Type, val any) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	if t == TYPE_TIMESTAMP_S {
		return ti.Unix(), nil
	}
	if t == TYPE_TIMESTAMP_MS {
		return ti.UnixMilli(), nil
	}

	year := ti.Year()
	if t == TYPE_TIMESTAMP || t == TYPE_TIMESTAMP_TZ {
		if year < -290307 || year > 294246 {
			return 0, conversionError(year, -290307, 294246)
		}
		return ti.UnixMicro(), nil
	}

	// TYPE_TIMESTAMP_NS:
	if year < 1678 || year > 2262 {
		return 0, conversionError(year, -290307, 294246)
	}
	return ti.UnixNano(), nil
}

func inferTimestamp(t Type, val any) (mapping.Timestamp, error) {
	ticks, err := getTSTicks(t, val)
	return mapping.NewTimestamp(ticks), err
}

func inferTimestampS(val any) (mapping.TimestampS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_S, val)
	return mapping.NewTimestampS(ticks), err
}

func inferTimestampMS(val any) (mapping.TimestampMS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_MS, val)
	return mapping.NewTimestampMS(ticks), err
}

func inferTimestampNS(val any) (mapping.TimestampNS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_NS, val)
	return mapping.NewTimestampNS(ticks), err
}

func inferDate[T any](val T) (mapping.Date, error) {
	ti, err := castToTime(val)
	if err != nil {
		return mapping.Date{}, err
	}

	date := mapping.NewDate(int32(ti.Unix() / secondsPerDay))
	return date, err
}

func inferTime(val any) (mapping.Time, error) {
	ticks, err := getTimeTicks(val)
	if err != nil {
		return mapping.Time{}, err
	}
	return mapping.NewTime(ticks), nil
}

func inferTimeTZ(val any) (mapping.TimeTZ, error) {
	ti, err := castToTime(val)
	if err != nil {
		return mapping.TimeTZ{}, err
	}

	// DuckDB stores time as microseconds since 00:00:00.
	base := time.Date(1970, time.January, 1, ti.Hour(), ti.Minute(), ti.Second(), ti.Nanosecond(), time.UTC)
	ticks := base.UnixMicro()

	// Preserve the UTC offset from the input time.
	_, offset := ti.Zone()
	return mapping.CreateTimeTZ(ticks, int32(offset)), nil
}

func getTimeTicks[T any](val T) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	// DuckDB stores time as microseconds since 00:00:00.
	base := time.Date(1970, time.January, 1, ti.Hour(), ti.Minute(), ti.Second(), ti.Nanosecond(), time.UTC)
	return base.UnixMicro(), err
}
