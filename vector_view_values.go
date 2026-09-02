package duckdb

// Int128 is a fixed-width signed 128-bit integer. Its value is
// Upper*2^64 + Lower.
type Int128 struct {
	Lower uint64
	Upper int64
}

// Int64 returns the value as an int64 and whether it is representable as one.
func (value Int128) Int64() (int64, bool) {
	converted := int64(value.Lower)
	if value.Upper == converted>>63 {
		return converted, true
	}
	return 0, false
}

// Uint128 is a fixed-width unsigned 128-bit integer. Its value is
// Upper*2^64 + Lower.
type Uint128 struct {
	Lower uint64
	Upper uint64
}

// Uint64 returns the value as a uint64 and whether it is representable as one.
func (value Uint128) Uint64() (uint64, bool) {
	if value.Upper == 0 {
		return value.Lower, true
	}
	return 0, false
}

// DecimalValue is an allocation-free representation of a DuckDB DECIMAL.
// Unscaled contains the integer value before applying Scale. For example,
// Unscaled=12345 and Scale=2 represents 123.45.
type DecimalValue struct {
	Unscaled Int128
	Width    uint8
	Scale    uint8
}

// TimeTZValue is an allocation-free TIME WITH TIME ZONE value. Micros is the
// time of day in microseconds and OffsetSeconds is the UTC offset in seconds.
type TimeTZValue struct {
	Micros        int64
	OffsetSeconds int32
}

// UnscaledInt64 returns the unscaled value as an int64 and whether it is
// representable as one.
func (value DecimalValue) UnscaledInt64() (int64, bool) {
	return value.Unscaled.Int64()
}

// RawJSON is an allocation-free view of JSON text stored in a DuckDB vector.
// Its contents are valid only while the underlying vector remains valid and
// unchanged.
type RawJSON struct {
	data string
}

// String returns the JSON text. The returned string has the same lifetime as
// the underlying vector.
func (value RawJSON) String() string {
	return value.data
}
