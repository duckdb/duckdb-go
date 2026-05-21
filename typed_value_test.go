package duckdb

import (
	"database/sql/driver"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type typedIntValuer int

func (v typedIntValuer) Value() (driver.Value, error) {
	return int64(v), nil
}

type typedSignedID int64

type typedUnsignedID uint16

type typedErrValuer struct{}

func (typedErrValuer) Value() (driver.Value, error) {
	return nil, errors.New("typed valuer error")
}

func TestTypedTimestampNSForComplexPreparedQuery(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`
		CREATE OR REPLACE TABLE events_ns(
			ts TIMESTAMP_NS,
			category VARCHAR
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO events_ns VALUES
			(TIMESTAMP_NS '2024-04-05 00:00:00.000000001', 'A'),
			(TIMESTAMP_NS '2024-04-05 01:00:00.000000001', 'A'),
			(TIMESTAMP_NS '2024-04-05 02:00:00.000000001', 'A'),
			(TIMESTAMP_NS '2024-04-06 00:00:00.000000001', 'A'),
			(TIMESTAMP_NS '2024-04-06 01:00:00.000000001', 'A')
	`)
	require.NoError(t, err)

	query := `
		SELECT
			COALESCE(base.category, cmp.category) AS category,
			base.metric_value AS metric_value,
			cmp.metric_value AS metric_value_prev,
			base.metric_value - cmp.metric_value AS metric_delta
		FROM (
			SELECT * FROM (VALUES (?, ?)) t(category, metric_value)
		) base
		LEFT JOIN (
			SELECT category, COUNT(*) AS metric_value
			FROM events_ns
			WHERE ts >= ? AND ts < ?
			  AND category IN (?)
			GROUP BY 1
		) cmp
		ON base.category IS NOT DISTINCT FROM cmp.category
		ORDER BY metric_value DESC NULLS LAST
		LIMIT 9
	`
	stmt, err := db.Prepare(query)
	require.NoError(t, err)
	defer closePreparedWrapper(t, stmt)

	start := time.Date(2024, time.April, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, time.April, 6, 0, 0, 0, 0, time.UTC)

	t.Run("bare time.Time still binds as timestamptz", func(t *testing.T) {
		var category string
		var metricValue int
		var metricValuePrev int64
		var metricDelta int64

		err := stmt.QueryRow("A", 2, start, end, "A").Scan(
			&category,
			&metricValue,
			&metricValuePrev,
			&metricDelta,
		)
		require.ErrorContains(t, err,
			"Cannot compare values of type TIMESTAMP_NS and type TIMESTAMP WITH TIME ZONE")
	})

	t.Run("explicit timestamp_ns wrapper succeeds", func(t *testing.T) {
		var category string
		var metricValue int
		var metricValuePrev int64
		var metricDelta int64

		err := stmt.QueryRow(
			"A",
			2,
			Typed(start, TYPE_TIMESTAMP_NS),
			Typed(end, TYPE_TIMESTAMP_NS),
			"A",
		).Scan(
			&category,
			&metricValue,
			&metricValuePrev,
			&metricDelta,
		)
		require.NoError(t, err)
		require.Equal(t, "A", category)
		require.Equal(t, 2, metricValue)
		require.EqualValues(t, 3, metricValuePrev)
		require.EqualValues(t, -1, metricDelta)
	})
}

func TestTypedRepresentativeScalars(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`SET TimeZone = 'UTC'`)
	require.NoError(t, err)

	t.Run("integer coerces from default int", func(t *testing.T) {
		var got int32
		err := db.QueryRow(`SELECT ?`, Typed(1, TYPE_INTEGER)).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, int32(1), got)
	})

	t.Run("integer accepts named aliases", func(t *testing.T) {
		var signed int64
		var unsigned uint32
		err := db.QueryRow(
			`SELECT ?, ?`,
			Typed(typedSignedID(42), TYPE_BIGINT),
			Typed(typedUnsignedID(7), TYPE_UINTEGER),
		).Scan(&signed, &unsigned)
		require.NoError(t, err)
		require.Equal(t, int64(42), signed)
		require.Equal(t, uint32(7), unsigned)
	})

	t.Run("float binds from default float", func(t *testing.T) {
		var got float32
		err := db.QueryRow(`SELECT ?`, Typed(1.25, TYPE_FLOAT)).Scan(&got)
		require.NoError(t, err)
		require.InDelta(t, 1.25, got, 0.0001)
	})

	t.Run("float preserves nan and infinities", func(t *testing.T) {
		tests := []struct {
			name  string
			value float64
			check func(float32) bool
		}{
			{name: "nan", value: math.NaN(), check: func(v float32) bool { return math.IsNaN(float64(v)) }},
			{name: "positive infinity", value: math.Inf(1), check: func(v float32) bool { return math.IsInf(float64(v), 1) }},
			{name: "negative infinity", value: math.Inf(-1), check: func(v float32) bool { return math.IsInf(float64(v), -1) }},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var got float32
				err := db.QueryRow(`SELECT ?`, Typed(tc.value, TYPE_FLOAT)).Scan(&got)
				require.NoError(t, err)
				require.True(t, tc.check(got))
			})
		}
	})

	t.Run("timestamp", func(t *testing.T) {
		want := time.Date(2024, time.April, 5, 12, 34, 56, 123456000, time.UTC)
		var got time.Time
		err := db.QueryRow(`SELECT ?`, Typed(want, TYPE_TIMESTAMP)).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("date and time", func(t *testing.T) {
		want := time.Date(2024, time.April, 5, 12, 34, 56, 0, time.UTC)
		var gotDate time.Time
		var gotTime time.Time
		err := db.QueryRow(`SELECT ?, ?`, Typed(want, TYPE_DATE), Typed(want, TYPE_TIME)).Scan(&gotDate, &gotTime)
		require.NoError(t, err)
		require.Equal(t, time.Date(2024, time.April, 5, 0, 0, 0, 0, time.UTC), gotDate)
		require.Equal(t, time.Date(1, time.January, 1, 12, 34, 56, 0, time.UTC), gotTime)
	})

	t.Run("interval", func(t *testing.T) {
		want := Interval{Days: 10, Months: 4, Micros: 123456}
		var got Interval
		err := db.QueryRow(`SELECT ?`, Typed(want, TYPE_INTERVAL)).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

func TestTypedNullAndValuerBinding(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("nil binds null", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(nil, TYPE_INTEGER)).Scan(&got)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("sqlnull ignores value and valuer", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(typedErrValuer{}, TYPE_SQLNULL)).Scan(&got)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("sqlnull accepts nil", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(nil, TYPE_SQLNULL)).Scan(&got)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("valuer result is coerced", func(t *testing.T) {
		var got int32
		err := db.QueryRow(`SELECT ?`, Typed(typedIntValuer(42), TYPE_INTEGER)).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, int32(42), got)
	})

	t.Run("typed value pointer binds value", func(t *testing.T) {
		var got int32
		typed := Typed(42, TYPE_INTEGER)
		err := db.QueryRow(`SELECT ?`, &typed).Scan(&got)
		require.NoError(t, err)
		require.Equal(t, int32(42), got)
	})

	t.Run("valuer error is returned", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(typedErrValuer{}, TYPE_INTEGER)).Scan(&got)
		require.ErrorContains(t, err, "typed valuer error")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("nil typed value pointer binds null", func(t *testing.T) {
		var got any
		var typed *TypedValue
		err := db.QueryRow(`SELECT ?`, typed).Scan(&got)
		require.NoError(t, err)
		require.Nil(t, got)
	})
}

func TestTypedValidation(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("unsupported type", func(t *testing.T) {
		tests := []struct {
			name  string
			typ   Type
			value any
		}{
			{name: "list", typ: TYPE_LIST, value: []int32{1}},
			{name: "decimal", typ: TYPE_DECIMAL, value: Decimal{}},
			{name: "uuid", typ: TYPE_UUID, value: UUID{}},
			{name: "blob", typ: TYPE_BLOB, value: []byte{}},
			{name: "bit", typ: TYPE_BIT, value: Bit{}},
			{name: "hugeint", typ: TYPE_HUGEINT, value: int64(1)},
			{name: "variant", typ: TYPE_VARIANT, value: "hello"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var got any
				err := db.QueryRow(`SELECT ?`, Typed(tc.value, tc.typ)).Scan(&got)
				require.ErrorContains(t, err, "unsupported data type: "+typedValueTypeName(tc.typ))
				require.ErrorContains(t, err, "index: 1")
			})
		}
	})

	t.Run("integer range", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(256, TYPE_UTINYINT)).Scan(&got)
		require.ErrorContains(t, err, "cannot convert 256 to UTINYINT")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("negative integer to unsigned", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(-1, TYPE_UBIGINT)).Scan(&got)
		require.ErrorContains(t, err, "cannot convert -1 to UBIGINT")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("unsigned integer to signed overflow", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(uint64(math.MaxInt64)+1, TYPE_BIGINT)).Scan(&got)
		require.ErrorContains(t, err, "cannot convert 9223372036854775808 to BIGINT")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("integer type mismatch", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed("1", TYPE_INTEGER)).Scan(&got)
		require.ErrorContains(t, err, "cannot cast string to INTEGER")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("float32 finite overflow", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(math.MaxFloat32*2, TYPE_FLOAT)).Scan(&got)
		require.ErrorContains(t, err, "cannot convert")
		require.ErrorContains(t, err, "to FLOAT")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("float32 finite underflow to zero", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(1e-50, TYPE_FLOAT)).Scan(&got)
		require.ErrorContains(t, err, "cannot convert")
		require.ErrorContains(t, err, "to FLOAT")
		require.ErrorContains(t, err, "index: 1")
	})

	t.Run("float type mismatch", func(t *testing.T) {
		var got any
		err := db.QueryRow(`SELECT ?`, Typed(1, TYPE_DOUBLE)).Scan(&got)
		require.ErrorContains(t, err, "cannot cast int to DOUBLE")
		require.ErrorContains(t, err, "index: 1")
	})
}
