package duckdb

import (
	"bytes"
	"encoding/json"
	"math/big"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// HugeInt is a signed 128-bit integer with value Upper * 2^64 + Lower.
// It supports parameter binding and appends to HUGEINT, BIGNUM, and JSON columns.
// Typed vector access avoids conversion through *big.Int.
// Ordinary query results remain *big.Int.
type HugeInt struct {
	Lower uint64
	Upper int64
}

// MarshalJSON encodes the value as a JSON integer.
func (value HugeInt) MarshalJSON() ([]byte, error) {
	hi := mapping.NewHugeInt(value.Lower, value.Upper)
	return hugeIntToNative(&hi).MarshalJSON()
}

// UnmarshalJSON decodes a JSON integer in the signed 128-bit range.
// A JSON null leaves the value unchanged.
func (value *HugeInt) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var integer big.Int
	if err := json.Unmarshal(data, &integer); err != nil {
		return err
	}
	hi, err := hugeIntFromNative(&integer)
	if err != nil {
		return err
	}
	value.Lower, value.Upper = mapping.HugeIntMembers(&hi)
	return nil
}

// UHugeInt is an unsigned 128-bit integer with value Upper * 2^64 + Lower.
// It supports parameter binding and appends to UHUGEINT, BIGNUM, and JSON columns.
// Typed vector access avoids conversion through *big.Int.
// Ordinary query results remain *big.Int.
type UHugeInt struct {
	Lower uint64
	Upper uint64
}

// MarshalJSON encodes the value as a JSON integer.
func (value UHugeInt) MarshalJSON() ([]byte, error) {
	uhi := mapping.NewUHugeInt(value.Lower, value.Upper)
	return uhugeIntToNative(&uhi).MarshalJSON()
}

// UnmarshalJSON decodes a JSON integer in the unsigned 128-bit range.
// A JSON null leaves the value unchanged.
func (value *UHugeInt) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var integer big.Int
	if err := json.Unmarshal(data, &integer); err != nil {
		return err
	}
	uhi, err := uhugeIntFromNative(&integer)
	if err != nil {
		return err
	}
	value.Lower, value.Upper = mapping.UHugeIntMembers(&uhi)
	return nil
}
