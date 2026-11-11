package ducktype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type NullDuration struct {
	V     time.Duration
	Valid bool
}

func (n *NullDuration) Scan(src any) error {
	type Interval struct {
		Days   int32 `json:"days"`
		Months int32 `json:"months"`
		Micros int64 `json:"micros"`
	}

	if src == nil {
		n.V, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	switch v := src.(type) {
	case int64:
		n.V = time.Duration(v)
		return nil
	case float64:
		n.V = time.Duration(v)
		return nil
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("nullduration: failed to parse []byte %q as int64: %w", v, err)
		}
		n.V = time.Duration(parsed)
		return nil
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("nullduration: failed to parse duration string: %w", err)
		}
		n.V = parsed
		return nil
	// case duckdb.Interval:
	// TODO: we currently dont handle this type directly because it pulls in all the cgo dependencies.
	default:
		var (
			decoded Interval
		)
		n.Valid = false

		encoded, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("nullduration: cannot scan type %T into NullDuration: %w", src, err)
		}

		if err = json.Unmarshal(encoded, &decoded); err != nil {
			return fmt.Errorf("nullduration: cannot scan type %T into NullDuration: %w", src, err)
		}

		n.Valid = true
		n.V = time.Duration(decoded.Months)*24*30*time.Hour + time.Duration(decoded.Days)*24*time.Hour + time.Duration(decoded.Micros)*time.Microsecond
		return nil
	}
}

// Value implements the driver.Valuer interface.
// It returns nil if the value is not valid, otherwise it returns the int64
// value of the duration.
func (n NullDuration) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}

	return n.V.String(), nil
}
