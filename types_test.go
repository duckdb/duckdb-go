package duckdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// First, this test inserts all types (except UUID and DECIMAL) with the Appender.
// Then, it tests scanning these types.

type testTypesEnum string

const testTypesEnumSQL = `CREATE TYPE my_enum AS ENUM ('0', '1', '2')`

type testTypesStruct struct {
	A int32
	B string
}

type testTypesRow struct {
	Boolean_col      bool
	Tinyint_col      int8
	Smallint_col     int16
	Integer_col      int32
	Bigint_col       int64
	Utinyint_col     uint8
	Usmallint_col    uint16
	Uinteger_col     uint32
	Ubigint_col      uint64
	Float_col        float32
	Double_col       float64
	Timestamp_col    time.Time
	Date_col         time.Time
	Time_col         time.Time
	Interval_col     Interval
	Hugeint_col      *big.Int
	Uhugeint_col     *big.Int
	Bignum_col       *big.Int
	Varchar_col      string
	Blob_col         []byte
	Timestamp_s_col  time.Time
	Timestamp_ms_col time.Time
	Timestamp_ns_col time.Time
	Enum_col         testTypesEnum
	List_col         Composite[[]int32]
	Struct_col       Composite[testTypesStruct]
	Map_col          Map
	OrderedMap_col   OrderedMap
	Array_col        Composite[[3]int32]
	Time_tz_col      time.Time
	Timestamp_tz_col time.Time
	Json_col_map     Composite[map[string]any]
	Json_col_array   Composite[[]any]
	Json_col_string  string
	Json_col_bool    bool
	Json_col_float64 float64
}

const testTypesTableSQL = `CREATE TABLE test (
	Boolean_col BOOLEAN,
	Tinyint_col TINYINT,
	Smallint_col SMALLINT,
	Integer_col INTEGER,
	Bigint_col BIGINT,
	Utinyint_col UTINYINT,
	Usmallint_col USMALLINT,
	Uinteger_col UINTEGER,
	Ubigint_col UBIGINT,
	Float_col FLOAT,
	Double_col DOUBLE,
	Timestamp_col TIMESTAMP,
	Date_col DATE,
	Time_col TIME,
	Interval_col INTERVAL,
	Hugeint_col HUGEINT,
	Uhugeint_col UHUGEINT,
	Bignum_col BIGNUM,
	Varchar_col VARCHAR,
	Blob_col BLOB,
	Timestamp_s_col TIMESTAMP_S,
	Timestamp_ms_col TIMESTAMP_MS,
	Timestamp_ns_col TIMESTAMP_NS,
	Enum_col my_enum,
	List_col INTEGER[],
	Struct_col STRUCT(A INTEGER, B VARCHAR),
	Map_col MAP(INTEGER, VARCHAR),
	OrderedMap_col MAP(INTEGER, VARCHAR),
	Array_col INTEGER[3],
	Time_tz_col TIMETZ,
	Timestamp_tz_col TIMESTAMPTZ,
	Json_col_map JSON,
	Json_col_array JSON,
	Json_col_string JSON,
	Json_col_bool JSON,
	Json_col_float64 JSON
)`

func (r *testTypesRow) toUTC() {
	r.Timestamp_col = r.Timestamp_col.UTC()
	r.Timestamp_s_col = r.Timestamp_s_col.UTC()
	r.Timestamp_ms_col = r.Timestamp_ms_col.UTC()
	r.Timestamp_ns_col = r.Timestamp_ns_col.UTC()
	// Time_tz_col preserves timezone, no UTC conversion.
	r.Timestamp_tz_col = r.Timestamp_tz_col.UTC()
}

func (r *testTypesRow) normalizeBigInt() {
	// Normalize big.Int zero values to have consistent internal representation.
	// SetBytes on empty/zero data creates abs: [] while NewInt(0) creates abs: nil.
	if r.Bignum_col != nil && r.Bignum_col.Sign() == 0 {
		r.Bignum_col = big.NewInt(0)
	}
}

func testTypesGenerateRow[T require.TestingT](t T, i int) testTypesRow {
	// Get the timestamp for all TS columns.
	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"
	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", IST)
	require.NoError(t, err)

	// Get the DATE, TIME, and TIMETZ column values.
	dateUTC := time.Date(1992, time.September, 20, 0, 0, 0, 0, time.UTC)
	timeUTC := time.Date(1, time.January, 1, 11, 42, 7, 0, time.UTC)
	timeTZ := time.Date(1, time.January, 1, 11, 42, 7, 0, IST)

	var buffer bytes.Buffer
	for range i {
		buffer.WriteString("hello!")
	}
	varcharCol := buffer.String()

	listCol := Composite[[]int32]{
		[]int32{int32(i)},
	}
	structCol := Composite[testTypesStruct]{
		testTypesStruct{int32(i), "a" + strconv.Itoa(i)},
	}
	mapCol := Map{
		int32(i): "other_longer_val",
	}
	orderedMapCol := OrderedMap{}
	orderedMapCol.Set(int32(i), "other_longer_val")
	orderedMapCol.Set(int32(i+2), "even_longer_val")
	arrayCol := Composite[[3]int32]{
		[3]int32{int32(i), int32(i), int32(i)},
	}
	jsonMapCol := Composite[map[string]any]{
		map[string]any{
			"hello": float64(42),
			"world": float64(84),
		},
	}
	jsonArrayCol := Composite[[]any]{
		[]any{"hello", "world"},
	}

	return testTypesRow{
		i%2 == 1,
		int8(i % 127),
		int16(i % 32767),
		int32(2147483647 - i),
		int64(9223372036854775807 - i),
		uint8(i % 256),
		uint16(i % 65535),
		uint32(2147483647 - i),
		uint64(9223372036854775807 - i),
		float32(i),
		float64(i),
		ts,
		dateUTC,
		timeUTC,
		Interval{Days: 0, Months: int32(i), Micros: 0},
		big.NewInt(int64(i)),
		big.NewInt(int64(i)),
		big.NewInt(int64(i)),
		varcharCol,
		[]byte{'A', 'B'},
		ts,
		ts,
		ts,
		testTypesEnum(strconv.Itoa(i % 3)),
		listCol,
		structCol,
		mapCol,
		orderedMapCol,
		arrayCol,
		timeTZ,
		ts,
		jsonMapCol,
		jsonArrayCol,
		varcharCol,
		i%2 == 1,
		float64(i),
	}
}

func testTypesGenerateRows[T require.TestingT](t T, rowCount int) []testTypesRow {
	var expectedRows []testTypesRow
	for i := range rowCount {
		r := testTypesGenerateRow(t, i)
		expectedRows = append(expectedRows, r)
	}
	return expectedRows
}

func testTypesReset[T require.TestingT](t T, c *Connector) {
	_, err := sql.OpenDB(c).ExecContext(context.Background(), `DELETE FROM test`)
	require.NoError(t, err)
}

func testTypes[T require.TestingT](t T, db *sql.DB, a *Appender, expectedRows []testTypesRow) []testTypesRow {
	// Append the rows. We cannot append Composite types.
	for i := range expectedRows {
		r := &expectedRows[i]
		err := a.AppendRow(
			r.Boolean_col,
			r.Tinyint_col,
			r.Smallint_col,
			r.Integer_col,
			r.Bigint_col,
			r.Utinyint_col,
			r.Usmallint_col,
			r.Uinteger_col,
			r.Ubigint_col,
			r.Float_col,
			r.Double_col,
			r.Timestamp_col,
			r.Date_col,
			r.Time_col,
			r.Interval_col,
			r.Hugeint_col,
			r.Uhugeint_col,
			r.Bignum_col,
			r.Varchar_col,
			r.Blob_col,
			r.Timestamp_s_col,
			r.Timestamp_ms_col,
			r.Timestamp_ns_col,
			string(r.Enum_col),
			r.List_col.Get(),
			r.Struct_col.Get(),
			r.Map_col,
			r.OrderedMap_col,
			r.Array_col.Get(),
			r.Time_tz_col,
			r.Timestamp_tz_col,
			r.Json_col_map.Get(),
			r.Json_col_array.Get(),
			r.Json_col_string,
			r.Json_col_bool,
			r.Json_col_float64)
		require.NoError(t, err)
	}
	require.NoError(t, a.Flush())

	res, err := db.QueryContext(context.Background(), `SELECT * FROM test ORDER BY Smallint_col`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	// Scan the rows.
	var actualRows []testTypesRow
	for res.Next() {
		var r testTypesRow
		err = res.Scan(
			&r.Boolean_col,
			&r.Tinyint_col,
			&r.Smallint_col,
			&r.Integer_col,
			&r.Bigint_col,
			&r.Utinyint_col,
			&r.Usmallint_col,
			&r.Uinteger_col,
			&r.Ubigint_col,
			&r.Float_col,
			&r.Double_col,
			&r.Timestamp_col,
			&r.Date_col,
			&r.Time_col,
			&r.Interval_col,
			&r.Hugeint_col,
			&r.Uhugeint_col,
			&r.Bignum_col,
			&r.Varchar_col,
			&r.Blob_col,
			&r.Timestamp_s_col,
			&r.Timestamp_ms_col,
			&r.Timestamp_ns_col,
			&r.Enum_col,
			&r.List_col,
			&r.Struct_col,
			&r.Map_col,
			&r.OrderedMap_col,
			&r.Array_col,
			&r.Time_tz_col,
			&r.Timestamp_tz_col,
			&r.Json_col_map,
			&r.Json_col_array,
			&r.Json_col_string,
			&r.Json_col_bool,
			&r.Json_col_float64)
		require.NoError(t, err)
		actualRows = append(actualRows, r)
	}

	require.NoError(t, err)
	require.Len(t, actualRows, len(expectedRows))
	return actualRows
}

func TestTypes(t *testing.T) {
	for _, appenderType := range appenderTypes {
		func() {
			expectedRows := testTypesGenerateRows(t, 3)
			c, db, conn, a := prepareAppender(t, appenderType, testTypesEnumSQL+";"+testTypesTableSQL)
			defer cleanupAppender(t, c, db, conn, a)
			actualRows := testTypes(t, db, a, expectedRows)

			for i := range actualRows {
				expectedRows[i].toUTC()
				// Time_tz_col preserves timezone, compare using Equal() which compares instants.
				require.True(t, expectedRows[i].Time_tz_col.Equal(actualRows[i].Time_tz_col),
					"Time_tz_col mismatch: expected %v, got %v", expectedRows[i].Time_tz_col, actualRows[i].Time_tz_col)
				// Set to same value for struct comparison.
				actualRows[i].Time_tz_col = expectedRows[i].Time_tz_col
				actualRows[i].normalizeBigInt()
				require.Equal(t, expectedRows[i], actualRows[i])
			}
			require.Len(t, actualRows, len(expectedRows))
		}()
	}
}

// NOTE: duckdb-go only contains very few benchmarks. The purpose of those benchmarks is to avoid regressions
// of its main functionalities. I.e., functions related to implementing the database/sql interface.
var benchmarkTypesResult []testTypesRow

func BenchmarkTypes(b *testing.B) {
	expectedRows := testTypesGenerateRows(b, GetDataChunkCapacity()*3+10)
	c, db, conn, a := prepareAppender(b, appenderTypeDefault, testTypesEnumSQL+";"+testTypesTableSQL)
	defer cleanupAppender(b, c, db, conn, a)

	var r []testTypesRow
	b.ResetTimer()
	for b.Loop() {
		r = testTypes(b, db, a, expectedRows)
		testTypesReset(b, c)
	}
	b.StopTimer()

	// Ensure that the compiler does not eliminate the line by storing the result.
	benchmarkTypesResult = r
}

func compareDecimal(t *testing.T, want, got Decimal) {
	require.Equal(t, want.Scale, got.Scale)
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, want.Value.String(), got.Value.String())
}

func TestDecimal(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT all possible DECIMAL widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			r := db.QueryRow(fmt.Sprintf(`SELECT 0::DECIMAL(%d, 1)`, i))
			var actual Decimal
			require.NoError(t, r.Scan(&actual))
			expected := Decimal{Width: uint8(i), Value: big.NewInt(0), Scale: 1}
			require.Equal(t, expected, actual)
		}
	})

	t.Run("SELECT different DECIMAL types", func(t *testing.T) {
		res, err := db.Query(`SELECT * FROM (VALUES
			(1.23::DECIMAL(3, 2)),
			(-1.23::DECIMAL(3, 2)),
			(123.45::DECIMAL(5, 2)),
			(-123.45::DECIMAL(5, 2)),
			(123456789.01::DECIMAL(11, 2)),
			(-123456789.01::DECIMAL(11, 2)),
			(1234567890123456789.234::DECIMAL(22, 3)),
			(-1234567890123456789.234::DECIMAL(22, 3)),
		) v
		ORDER BY v ASC`)
		require.NoError(t, err)
		defer closeRowsWrapper(t, res)

		bigNumber, success := new(big.Int).SetString("1234567890123456789234", 10)
		require.True(t, success)
		bigNegativeNumber, success := new(big.Int).SetString("-1234567890123456789234", 10)
		require.True(t, success)
		tests := []struct {
			input string
			want  Decimal
		}{
			{input: "1.23::DECIMAL(3, 2)", want: Decimal{Value: big.NewInt(123), Width: 3, Scale: 2}},
			{input: "-1.23::DECIMAL(3, 2)", want: Decimal{Value: big.NewInt(-123), Width: 3, Scale: 2}},
			{input: "123.45::DECIMAL(5, 2)", want: Decimal{Value: big.NewInt(12345), Width: 5, Scale: 2}},
			{input: "-123.45::DECIMAL(5, 2)", want: Decimal{Value: big.NewInt(-12345), Width: 5, Scale: 2}},
			{input: "123456789.01::DECIMAL(11, 2)", want: Decimal{Value: big.NewInt(12345678901), Width: 11, Scale: 2}},
			{input: "-123456789.01::DECIMAL(11, 2)", want: Decimal{Value: big.NewInt(-12345678901), Width: 11, Scale: 2}},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: Decimal{Value: bigNumber, Width: 22, Scale: 3}},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: Decimal{Value: bigNegativeNumber, Width: 22, Scale: 3}},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf(`SELECT %s`, test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			compareDecimal(t, test.want, fs)
		}
	})

	t.Run("SELECT a huge DECIMAL ", func(t *testing.T) {
		bigInt, success := new(big.Int).SetString("12345678901234567890123456789", 10)
		require.True(t, success)
		var f Decimal
		require.NoError(t, db.QueryRow("SELECT 123456789.01234567890123456789::DECIMAL(29, 20)").Scan(&f))
		compareDecimal(t, Decimal{Value: bigInt, Width: 29, Scale: 20}, f)
	})

	t.Run("SELECT DECIMAL types and compare them to FLOAT64", func(t *testing.T) {
		tests := []struct {
			input string
			want  float64
		}{
			{input: "1.23::DECIMAL(3, 2)", want: 1.23},
			{input: "-1.23::DECIMAL(3, 2)", want: -1.23},
			{input: "123.45::DECIMAL(5, 2)", want: 123.45},
			{input: "-123.45::DECIMAL(5, 2)", want: -123.45},
			{input: "123456789.01::DECIMAL(11, 2)", want: 123456789.01},
			{input: "-123456789.01::DECIMAL(11, 2)", want: -123456789.01},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: 1234567890123456789.234},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: -1234567890123456789.234},
			{input: "123456789.01234567890123456789::DECIMAL(29, 20)", want: 123456789.01234567890123456789},
			{input: "-123456789.01234567890123456789::DECIMAL(29, 20)", want: -123456789.01234567890123456789},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf("SELECT %s", test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			require.Equal(t, test.want, fs.Float64())
		}
	})

	t.Run("SELECT DECIMAL types and compare them to STRING", func(t *testing.T) {
		tests := []struct {
			input string
			want  string
		}{
			{input: "1.23::DECIMAL(3, 2)", want: "1.23"},
			{input: "-1.23::DECIMAL(3, 2)", want: "-1.23"},
			{input: "123.45::DECIMAL(5, 2)", want: "123.45"},
			{input: "-123.45::DECIMAL(5, 2)", want: "-123.45"},
			{input: "123456789.01::DECIMAL(11, 2)", want: "123456789.01"},
			{input: "-123456789.01::DECIMAL(11, 2)", want: "-123456789.01"},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: "1234567890123456789.234"},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: "-1234567890123456789.234"},
			{input: "123456789.01234567890123456789::DECIMAL(29, 20)", want: "123456789.01234567890123456789"},
			{input: "-123456789.01234567890123456789::DECIMAL(29, 20)", want: "-123456789.01234567890123456789"},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf("SELECT %s", test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			require.Equal(t, test.want, fs.String())
			// confirms Decimal implements fmt.Stringer correctly (see #424)
			require.Equal(t, test.want, fmt.Sprint(fs))
		}
	})
}

func TestDecimalString(t *testing.T) {
	testCases := []struct {
		input    Decimal
		expected string
	}{
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(0),
			},
			expected: "0",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(0),
			},
			expected: "0",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(1234567890),
			},
			expected: "1234567890",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(-1234567890),
			},
			expected: "-1234567890",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(1234567890),
			},
			expected: "123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(-1234567890),
			},
			expected: "-123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 2,
				Value: big.NewInt(1234567890),
			},
			expected: "12345678.9",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 2,
				Value: big.NewInt(-1234567890),
			},
			expected: "-12345678.9",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(1234567890),
			},
			expected: "1234.56789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(-1234567890),
			},
			expected: "-1234.56789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 12,
				Value: big.NewInt(1234567890),
			},
			expected: "0.00123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 12,
				Value: big.NewInt(-1234567890),
			},
			expected: "-0.00123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(1234500000),
			},
			expected: "123450000",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(-1234500000),
			},
			expected: "-123450000",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 8,
				Value: big.NewInt(-705399),
			},
			expected: "-0.00705399",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 8,
				Value: big.NewInt(821662),
			},
			expected: "0.00821662",
		},
	}

	for _, tc := range testCases {
		actual := tc.input.String()
		if actual != tc.expected {
			require.Equal(t, tc.expected, actual)
		}
	}
}

func TestBit(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT different BIT values", func(t *testing.T) {
		tests := []string{
			"10101",
			"0",
			"1",
			"00000000",
			"11111111",
			"100000001",
			// Multi-byte tests
			"1010101010101010",                 // 16 bits (2 bytes)
			"10101010101010101",                // 17 bits (3 bytes with padding)
			"10101010101010101010101010101010", // 32 bits (4 bytes)
			"111100001100110011",               // 18 bits (3 bytes with 6 padding)
			"0000000011111111001",              // 19 bits (3 bytes with 5 padding)
			"1101101001011",                    // 13 bits (2 bytes with 3 padding)
			"00000000000000000000000010110101", // 30 bits (4 bytes with 2 padding)
		}
		for _, bits := range tests {
			var res Bit
			err := db.QueryRow(fmt.Sprintf("SELECT '%s'::BIT", bits)).Scan(&res)
			require.NoError(t, err, "failed for input %s", bits)
			require.Equal(t, bits, res.String(), "mismatch for input %s", bits)
		}
	})

	t.Run("BitFromData", func(t *testing.T) {
		// Multi-byte: 10 bits "1010101011" = padding=6, [11111110 10101011]
		b := Bit{Data: []byte{6, 0xFE, 0xAB}}
		require.Equal(t, 10, b.Len())
		require.Equal(t, "1010101011", b.String())

		// Byte-aligned: 0xAA = 10101010 (no padding)
		b8 := Bit{Data: []byte{0, 0xAA}}
		require.Equal(t, 8, b8.Len())
		require.Equal(t, "10101010", b8.String())

		// nil returns empty Bit
		bNil := Bit{}
		require.Equal(t, 0, bNil.Len())

		// Single byte (just padding count, no data) returns empty Bit
		bEmpty := Bit{Data: []byte{0}}
		require.Equal(t, 0, bEmpty.Len())

		// Malformed values should not panic when stringified.
		bInvalid := Bit{Data: []byte{7}}
		require.Equal(t, 0, bInvalid.Len())
		require.Empty(t, bInvalid.String())
	})

	t.Run("Validate", func(t *testing.T) {
		require.ErrorContains(t, Bit{}.Validate(), "empty bit string")
		require.ErrorContains(t, (Bit{Data: []byte{0}}).Validate(), "empty bit string")
		require.NoError(t, (Bit{Data: []byte{0, 0xAA}}).Validate())
		require.NoError(t, (Bit{Data: []byte{6, 0xFE, 0xAB}}).Validate())

		// Invalid padding count (> 7)
		require.ErrorContains(t, (Bit{Data: []byte{8, 0xAA}}).Validate(), "invalid padding count")

		// Padding bits not set to 1
		require.ErrorContains(t, (Bit{Data: []byte{6, 0x3E, 0xAB}}).Validate(), "padding bits must be 1s")
	})

	t.Run("NewBitFromString", func(t *testing.T) {
		// Empty string returns an error.
		_, err := NewBitFromString("")
		require.ErrorContains(t, err, "empty bit string")

		// Invalid characters
		_, err = NewBitFromString("10102")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid character")
	})

	t.Run("BIT scan compatibility", func(t *testing.T) {
		bits := "10101010101010101"

		var s string
		err := db.QueryRow(fmt.Sprintf("SELECT '%s'::BIT", bits)).Scan(&s)
		require.NoError(t, err)
		require.Equal(t, bits, s)

		var raw []byte
		err = db.QueryRow(fmt.Sprintf("SELECT '%s'::BIT", bits)).Scan(&raw)
		require.NoError(t, err)
		require.Equal(t, []byte(bits), raw)

		var v any
		err = db.QueryRow(fmt.Sprintf("SELECT '%s'::BIT", bits)).Scan(&v)
		require.NoError(t, err)
		require.Equal(t, bits, v)
	})

	t.Run("BIT binding", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE bit_bind_test (bits BIT)")
		require.NoError(t, err)

		tests := []string{
			"11001100",
			"111100001010010110110100",
		}
		for _, bits := range tests {
			bitVal, err := NewBitFromString(bits)
			require.NoError(t, err)

			_, err = db.Exec("INSERT INTO bit_bind_test VALUES(?)", bitVal)
			require.NoError(t, err)

			// Also test binding *Bit.
			_, err = db.Exec("INSERT INTO bit_bind_test VALUES(?)", &bitVal)
			require.NoError(t, err)

			var res Bit
			err = db.QueryRow("SELECT bits FROM bit_bind_test WHERE bits = ?", bitVal).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, bits, res.String())
		}

		// Test binding nil *Bit.
		var nilBit *Bit
		_, err = db.Exec("INSERT INTO bit_bind_test VALUES(?)", nilBit)
		require.NoError(t, err)

		var res *Bit
		err = db.QueryRow("SELECT bits FROM bit_bind_test WHERE bits IS NULL").Scan(&res)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}

func TestBlob(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Scan a hexadecimal value.
	var b []byte
	require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&b))
	require.Equal(t, []byte{0xAA}, b)
}

func TestList(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Test a LIST exceeding duckdb's standard vector size.
	const n = 4000
	var row Composite[[]int]
	require.NoError(t, db.QueryRow("SELECT range(0, ?, 1)", n).Scan(&row))
	require.Len(t, row.Get(), n)
	for i := range n {
		require.Equal(t, i, row.Get()[i])
	}
}

func TestUUID(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE uuid_test(uuid UUID)`)
	require.NoError(t, err)

	tests := []uuid.UUID{
		uuid.New(),
		uuid.Nil,
		uuid.MustParse("80000000-0000-0000-0000-200000000000"),
	}
	for _, test := range tests {
		_, err = db.Exec(`INSERT INTO uuid_test VALUES(?)`, test)
		require.NoError(t, err)

		var val uuid.UUID
		require.NoError(t, db.QueryRow(`SELECT uuid FROM uuid_test WHERE uuid = ?`, test).Scan(&val))
		require.Equal(t, test, val)

		require.NoError(t, db.QueryRow(`SELECT ?`, test).Scan(&val))
		require.Equal(t, test, val)

		require.NoError(t, db.QueryRow(`SELECT ?::uuid`, test).Scan(&val))
		require.Equal(t, test, val)

		var u UUID
		require.NoError(t, db.QueryRow(`SELECT uuid FROM uuid_test WHERE uuid = ?`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())

		require.NoError(t, db.QueryRow(`SELECT ?`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())

		require.NoError(t, db.QueryRow(`SELECT ?::uuid`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())
	}
}

func TestUUIDScanError(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var u UUID
	// invalid value type
	require.Error(t, db.QueryRow(`SELECT 12345`).Scan(&u))
	// string value not valid
	require.Error(t, db.QueryRow(`SELECT 'I am not a UUID.'`).Scan(&u))
	// blob value not valid
	require.Error(t, db.QueryRow(`SELECT '123456789012345678901234567890123456'::BLOB`).Scan(&u))
}

func TestDate(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	tests := map[string]struct {
		want  time.Time
		input string
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, time.December, 12, 0, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as DATE)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}

	ts, err := time.Parse(time.DateTime, time.DateTime)
	require.NoError(t, err)

	var res time.Time
	err = db.QueryRow(`SELECT ?::DATE`, ts).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, time.Date(2006, time.January, 0o2, 0, 0, 0, 0, time.UTC), res)
}

func TestTime(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	timeUTC := time.Date(1, time.January, 1, 11, 42, 7, 0, time.UTC)

	var res time.Time
	err = db.QueryRow(`SELECT ?::TIME`, timeUTC).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, timeUTC, res)

	timeTZ := time.Date(1, time.January, 1, 11, 42, 7, 0, IST)

	err = db.QueryRow(`SELECT ?::TIMETZ`, timeTZ).Scan(&res)
	require.NoError(t, err)
	// TIMETZ preserves the time and timezone offset.
	require.True(t, timeTZ.Equal(res), "expected %v, got %v", timeTZ, res)
}

func TestENUMs(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	type environment string
	const (
		Sea  environment = "Sea"
		Air  environment = "Air"
		Land environment = "Land"
	)

	_, err := db.Exec("CREATE TYPE element AS ENUM ('Sea', 'Air', 'Land')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE vehicles (name text, environment element)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO vehicles VALUES (?, ?), (?, ?)", "Aircraft", Air, "Boat", Sea)
	require.NoError(t, err)

	var name string
	var env environment
	require.NoError(t, db.QueryRow("SELECT name, environment FROM vehicles WHERE environment = ?", Air).Scan(&name, &env))
	require.Equal(t, "Aircraft", name)
	require.Equal(t, Air, env)

	_, err = db.Exec("CREATE TABLE all_enums (environments element[])")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO all_enums VALUES ([?, ?, ?])", Air, Land, Sea)
	require.NoError(t, err)

	var row Composite[[]environment]
	require.NoError(t, db.QueryRow("SELECT environments FROM all_enums").Scan(&row))
	require.ElementsMatch(t, []environment{Air, Sea, Land}, row.Get())
}

// TestEnumNullValues verifies that NULL ENUM cells are read back as nil.
func TestEnumNullValues(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec("CREATE TYPE nullable_color AS ENUM ('red', 'green', 'blue')")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE nullable_colors (val nullable_color)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO nullable_colors VALUES ('red'), (NULL), ('blue')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT val FROM nullable_colors ORDER BY rowid")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []any{"red", nil, "blue"}
	for _, exp := range expected {
		require.True(t, rows.Next())
		var val any
		require.NoError(t, rows.Scan(&val))
		require.Equal(t, exp, val)
	}
}

// TestEnumLargeDictionary verifies correctness when the ENUM dictionary exceeds
// 255 entries, forcing DuckDB to use USMALLINT as the internal storage type.
// This exercises the TYPE_USMALLINT branch in getEnum.
func TestEnumLargeDictionary(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Build 300 unique enum values: "v0", "v1", ..., "v299"
	// DuckDB uses UTINYINT for ≤255 values and USMALLINT for 256–65535.
	values := make([]string, 300)
	for i := range values {
		values[i] = fmt.Sprintf("'v%d'", i)
	}
	createSQL := fmt.Sprintf("CREATE TYPE large_enum AS ENUM (%s)", strings.Join(values, ", "))
	_, err := db.Exec(createSQL)
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE large_enum_tbl (val large_enum)")
	require.NoError(t, err)

	// Insert first, last, and a middle value to cover boundary indices.
	_, err = db.Exec("INSERT INTO large_enum_tbl VALUES ('v0'), ('v255'), ('v299')")
	require.NoError(t, err)

	rows, err := db.Query("SELECT val FROM large_enum_tbl ORDER BY rowid")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []string{"v0", "v255", "v299"}
	for _, exp := range expected {
		require.True(t, rows.Next())
		var val string
		require.NoError(t, rows.Scan(&val))
		require.Equal(t, exp, val)
	}
}

func TestHugeInt(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT different HUGEINT values", func(t *testing.T) {
		tests := []string{
			"0",
			"1",
			"-1",
			"9223372036854775807",
			"-9223372036854775808",
			"170141183460469231731687303715884105727",
			"-170141183460469231731687303715884105727",
		}
		for _, test := range tests {
			var res *big.Int
			err := db.QueryRow(fmt.Sprintf("SELECT %s::HUGEINT", test)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test, res.String())
		}
	})

	t.Run("HUGEINT binding", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE hugeint_test (number HUGEINT)")
		require.NoError(t, err)

		val := big.NewInt(1)
		val.SetBit(val, 101, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", val)
		require.NoError(t, err)

		var res *big.Int
		err = db.QueryRow("SELECT number FROM hugeint_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val.String(), res.String())

		tooHuge := big.NewInt(1)
		tooHuge.SetBit(tooHuge, 129, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", tooHuge)
		require.Error(t, err)
		require.Contains(t, err.Error(), "too big for HUGEINT")
	})
}

func TestUHugeInt(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT different UHUGEINT values", func(t *testing.T) {
		tests := []string{
			"0",
			"1",
			"9223372036854775807",
			"18446744073709551615",
			"340282366920938463463374607431768211455",
		}
		for _, test := range tests {
			var res *big.Int
			err := db.QueryRow(fmt.Sprintf("SELECT %s::UHUGEINT", test)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test, res.String())
		}
	})

	t.Run("UHUGEINT binding", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE uhugeint_test (number UHUGEINT)")
		require.NoError(t, err)

		val := big.NewInt(1)
		val.SetBit(val, 101, 1)
		_, err = db.Exec("INSERT INTO uhugeint_test VALUES(?)", val)
		require.NoError(t, err)

		var res *big.Int
		err = db.QueryRow("SELECT number FROM uhugeint_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val.String(), res.String())

		tooHuge := big.NewInt(1)
		tooHuge.SetBit(tooHuge, 129, 1)
		_, err = db.Exec("INSERT INTO uhugeint_test VALUES(?)", tooHuge)
		require.Error(t, err)
		require.Contains(t, err.Error(), "too big for UHUGEINT")
	})

	t.Run("negative value rejected", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE uhugeint_neg_test (number UHUGEINT)")
		require.NoError(t, err)

		negVal := big.NewInt(-1)
		_, err = db.Exec("INSERT INTO uhugeint_neg_test VALUES(?)", negVal)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative")
	})
}

func TestTimestampTZ(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS tbl (tz TIMESTAMPTZ)`)
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"

	// Test a location east of GMT.
	loc, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", loc)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tbl (tz) VALUES(?)`, ts)
	require.NoError(t, err)

	var tz time.Time
	err = db.QueryRow(`SELECT tz FROM tbl`).Scan(&tz)
	require.NoError(t, err)
	require.Equal(t, ts.UTC(), tz.UTC())

	// Reset and test a location west of GMT.
	_, err = db.Exec(`DELETE FROM tbl`)
	require.NoError(t, err)

	loc, err = time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	ts, err = time.ParseInLocation(longForm, "2016-01-17 10:04:05 PDT", loc)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tbl (tz) VALUES(?)`, ts)
	require.NoError(t, err)

	err = db.QueryRow(`SELECT tz FROM tbl`).Scan(&tz)
	require.NoError(t, err)
	require.Equal(t, ts.UTC(), tz.UTC())

	// Test other time zone.
	ti := time.Now().UTC().Truncate(time.Microsecond)

	_, err = db.Exec(`SET TimeZone = 'Etc/UTC'`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE ts_tbl (t TIMESTAMPTZ)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO ts_tbl VALUES (?)`, ti)
	require.NoError(t, err)

	var newTime time.Time
	require.NoError(t, db.QueryRow(`SELECT t FROM ts_tbl`).Scan(&newTime))
	require.Equal(t, ti, newTime)

	// Test disabling TIMESTAMP_TZ casts.

	_, err = db.Exec(`SET disable_timestamptz_casts = true`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE times (t TIMESTAMPTZ)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO times VALUES (?)`, ti)
	require.NoError(t, err)
}

func TestBoolean(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var res bool
	require.NoError(t, db.QueryRow("SELECT ?", true).Scan(&res))
	require.True(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", false).Scan(&res))
	require.False(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", 0).Scan(&res))
	require.False(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", 1).Scan(&res))
	require.True(t, res)
}

func TestTimestamp(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":         {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970":   {input: "1950-12-12", want: time.Date(1950, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":    {input: "2022-12-12", want: time.Date(2022, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"HH:MM:SS":      {input: "2022-12-12 11:35:43", want: time.Date(2022, time.December, 12, 11, 35, 43, 0, time.UTC)},
		"HH:MM:SS.DDDD": {input: "2022-12-12 11:35:43.5678", want: time.Date(2022, time.December, 12, 11, 35, 43, 567800000, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as TIMESTAMP)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}
}

func TestInterval(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("INTERVAL binding", func(t *testing.T) {
		interval := Interval{Days: 10, Months: 4, Micros: 4}
		row := db.QueryRow("SELECT ?::INTERVAL", interval)

		var res Interval
		require.NoError(t, row.Scan(&res))
		require.Equal(t, interval, res)
	})

	t.Run("INTERVAL scanning", func(t *testing.T) {
		tests := map[string]struct {
			input string
			want  Interval
		}{
			"simple interval": {
				input: "INTERVAL 5 HOUR",
				want:  Interval{Days: 0, Months: 0, Micros: 18000000000},
			},
			"interval arithmetic": {
				input: "INTERVAL 1 DAY + INTERVAL 5 DAY",
				want:  Interval{Days: 6, Months: 0, Micros: 0},
			},
			"timestamp arithmetic": {
				input: "CAST('2022-05-01' as TIMESTAMP) - CAST('2022-04-01' as TIMESTAMP)",
				want:  Interval{Days: 30, Months: 0, Micros: 0},
			},
		}
		for _, test := range tests {
			var res Interval
			err := db.QueryRow(fmt.Sprintf("SELECT %s", test.input)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test.want, res)
		}
	})
}

func TestArray(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE needle (vec FLOAT[3])`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO needle VALUES (array[5, 5, 5])`)
	require.NoError(t, err)

	res, err := db.Query(`SELECT vec FROM needle`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	for res.Next() {
		var vec Composite[[3]float64]
		err = res.Scan(&vec)
		require.NoError(t, err)
		require.NoError(t, res.Err())
		require.Equal(t, [3]float64{5, 5, 5}, vec.Get())
	}
}

func TestJSONType(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE test (c1 STRUCT(index INTEGER))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test VALUES ({index: 1}), ({index: 2}), ({index: 2}), ({index: 3}), ({index: 3}), ({index: 3})`)
	require.NoError(t, err)

	// Verify results.
	row := db.QueryRowContext(context.Background(), `
	SELECT json_group_object(t2.status, t2.count) AS result
	FROM (
		SELECT json_extract(c1, '$.index') AS status, COUNT(*) AS count
		FROM test
		GROUP BY status
	) AS t2`)

	var res Composite[map[string]any]
	require.NoError(t, row.Scan(&res))
	require.Equal(t, float64(1), res.Get()["1"])
	require.Equal(t, float64(2), res.Get()["2"])
	require.Equal(t, float64(3), res.Get()["3"])
}

func TestJSONColType(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE OR REPLACE TABLE test AS SELECT '[10]'::JSON AS col, 1 AS val`)
	require.NoError(t, err)

	res, err := db.QueryContext(context.Background(), `SELECT col AS value, count(*) AS count FROM test GROUP BY 1`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	columnTypes, err := res.ColumnTypes()
	require.NoError(t, err)

	require.Len(t, columnTypes, 2)
	require.Equal(t, aliasJSON, columnTypes[0].DatabaseTypeName())
	require.Equal(t, typeToStringMap[TYPE_BIGINT], columnTypes[1].DatabaseTypeName())
	require.Equal(t, reflectTypeAny, columnTypes[0].ScanType())
	require.Equal(t, reflectTypeInt64, columnTypes[1].ScanType())
}

func TestUnionTypes(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Test basic UNION type creation and scanning.
	t.Run("basic UNION operations", func(t *testing.T) {
		r, err := db.Query(`
            SELECT
                (123)::UNION(num INTEGER, str VARCHAR) AS int_union,
                ('hello')::UNION(num INTEGER, str VARCHAR) AS str_union,
                NULL::UNION(num INTEGER, str VARCHAR) AS null_union
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var intUnion, strUnion Union
		var nullUnion any
		err = r.Scan(&intUnion, &strUnion, &nullUnion)
		require.NoError(t, err)

		require.Equal(t, "num", intUnion.Tag)
		require.Equal(t, int32(123), intUnion.Value)

		require.Equal(t, "str", strUnion.Tag)
		require.Equal(t, "hello", strUnion.Value)

		require.Nil(t, nullUnion)
	})

	// Test UNION with different types.
	t.Run("UNION with different types", func(t *testing.T) {
		r, err := db.Query(`
            WITH unions AS (
                SELECT
                    (1.5)::UNION(d DOUBLE, i INTEGER) AS double_union,
                    ('2024-01-01'::DATE)::UNION(d DATE, s VARCHAR) AS date_union
            )
            SELECT * FROM unions
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var doubleUnion, dateUnion Union
		err = r.Scan(&doubleUnion, &dateUnion)
		require.NoError(t, err)

		require.Equal(t, "d", doubleUnion.Tag)
		require.Equal(t, float64(1.5), doubleUnion.Value)

		require.Equal(t, "d", dateUnion.Tag)
		require.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), dateUnion.Value)
	})

	// Test column type information.
	t.Run("UNION column type info", func(t *testing.T) {
		r, err := db.Query(`
            SELECT (123)::UNION(num INTEGER, "a str" VARCHAR) AS union_col
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		types, err := r.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, "UNION(\"num\" INTEGER, \"a str\" VARCHAR)", types[0].DatabaseTypeName())
	})

	// Test multiple UNION members.
	t.Run("UNION with multiple members", func(t *testing.T) {
		r, err := db.Query(`
            SELECT (123)::UNION(a INTEGER, b VARCHAR, c DOUBLE) AS multi_union
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var val Union
		err = r.Scan(&val)
		require.NoError(t, err)
		require.Equal(t, "a", val.Tag)
		require.Equal(t, int32(123), val.Value)
	})
}

func TestInferPrimitiveType(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	testCases := []struct {
		input any
	}{
		{[]Map{nil}},
		{[]bool{true, false}},
		{[]int8{-7}},
		{[]int16{-42}},
		{[]int32{-4}},
		{[]int64{-6}},
		{[]int{-22}},
		{[]uint8{7}},
		{[]uint16{42}},
		{[]uint32{4}},
		{[]uint64{6}},
		{[]uint{22}},
		{[]float32{7.8}},
		{[]float64{22.3}},
		{[]string{"Hello from Amsterdam!"}},
		{[][]byte{{71, 111}}},
		{[]time.Time{time.Now()}},
		{[]Interval{{22, 10, 7}}},
		{[]*big.Int{big.NewInt(22)}},
		{[]Decimal{{2, 2, big.NewInt(7)}}},
		{[]UUID{UUID(uuid.New())}},
	}
	for _, tc := range testCases {
		_, err := db.Exec(`SELECT a FROM (VALUES (?)) t(a)`, tc.input)
		require.NoError(t, err)
	}

	// Not yet supported.
	testCases = []struct {
		input any
	}{
		{[]Union{{42, "n"}}},
		{[]Map{map[any]any{"hello": "world", "beautiful": "day"}}},
	}
	for _, tc := range testCases {
		_, err := db.Exec(`SELECT a FROM (VALUES (?)) t(a)`, tc.input)
		require.ErrorContains(t, err, unsupportedTypeErrMsg)
	}
}

func TestBigNum(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT different BIGNUM values", func(t *testing.T) {
		tests := []string{
			"0",
			"1",
			"-1",
			"9223372036854775807",
			"-9223372036854775808",
			"170141183460469231731687303715884105727",
			"-170141183460469231731687303715884105727",
			"340282366920938463463374607431768211455",
		}
		for _, test := range tests {
			var res *big.Int
			err := db.QueryRow(fmt.Sprintf("SELECT %s::BIGNUM", test)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test, res.String())
		}
	})

	t.Run("BIGNUM binding", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE bignum_test (number BIGNUM)")
		require.NoError(t, err)

		val := big.NewInt(1)
		val.SetBit(val, 101, 1)
		_, err = db.Exec("INSERT INTO bignum_test VALUES(?)", val)
		require.NoError(t, err)

		var res *big.Int
		err = db.QueryRow("SELECT number FROM bignum_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val.String(), res.String())
	})
}

func TestOrderedMap(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE ordered_map_test (data MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	orderedMap := OrderedMap{
		keys:   []any{"first", "second", "third"},
		values: []any{int32(1), int32(2), int32(3)},
	}

	_, err = db.Exec(`INSERT INTO ordered_map_test (data) VALUES (?)`, orderedMap)
	require.NoError(t, err)

	var result OrderedMap
	err = db.QueryRow(`SELECT data FROM ordered_map_test`).Scan(&result)
	require.NoError(t, err)

	require.Equal(t, orderedMap.Keys(), result.Keys())
	require.Equal(t, orderedMap.Values(), result.Values())
}

func TestMap(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE map_test (data MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	testMap := Map{
		"first":  int32(1),
		"second": int32(2),
		"third":  int32(3),
	}

	_, err = db.Exec(`INSERT INTO map_test (data) VALUES (?)`, testMap)
	require.NoError(t, err)

	var result Map
	err = db.QueryRow(`SELECT data FROM map_test`).Scan(&result)
	require.NoError(t, err)

	require.Equal(t, testMap, result)
}

func TestOrderedMapWhereClause(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE map_equality_test (id INTEGER, data MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	// Insert two rows with ordered maps
	orderedMap1 := OrderedMap{
		keys:   []any{"a", "b", "c"},
		values: []any{int32(1), int32(2), int32(3)},
	}
	orderedMap2 := OrderedMap{
		keys:   []any{"x", "y", "z"},
		values: []any{int32(7), int32(8), int32(9)},
	}

	_, err = db.Exec(`INSERT INTO map_equality_test (id, data) VALUES (?, ?)`, 1, orderedMap1)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO map_equality_test (id, data) VALUES (?, ?)`, 2, orderedMap2)
	require.NoError(t, err)

	// Test WHERE clause with exact map equality
	var resultID int
	var resultMap OrderedMap
	err = db.QueryRow(`SELECT id, data FROM map_equality_test WHERE data = ?`, orderedMap1).Scan(&resultID, &resultMap)
	require.NoError(t, err)
	require.Equal(t, 1, resultID)
	require.Equal(t, orderedMap1.Keys(), resultMap.Keys())
	require.Equal(t, orderedMap1.Values(), resultMap.Values())

	// Test that wrong order doesn't match
	wrongOrderMap := OrderedMap{
		keys:   []any{"c", "b", "a"}, // Different order
		values: []any{int32(3), int32(2), int32(1)},
	}
	err = db.QueryRow(`SELECT id, data FROM map_equality_test WHERE data = ?`, wrongOrderMap).Scan(&resultID, &resultMap)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestOrderedMapEmpty(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE empty_map_test (data MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	// Test empty OrderedMap
	emptyMap := OrderedMap{
		keys:   []any{},
		values: []any{},
	}

	_, err = db.Exec(`INSERT INTO empty_map_test (data) VALUES (?)`, emptyMap)
	require.NoError(t, err)

	var result OrderedMap
	err = db.QueryRow(`SELECT data FROM empty_map_test`).Scan(&result)
	require.NoError(t, err)

	require.Equal(t, 0, result.Len())
	require.Equal(t, emptyMap.Keys(), result.Keys())
	require.Equal(t, emptyMap.Values(), result.Values())
}

func TestOrderedMapNullValue(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE null_map_test (id INTEGER, data MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	// Test inserting a NULL map
	_, err = db.Exec(`INSERT INTO null_map_test (id, data) VALUES (?, ?)`, 1, nil)
	require.NoError(t, err)

	// Test inserting a regular map
	regularMap := OrderedMap{
		keys:   []any{"first", "second"},
		values: []any{int32(1), int32(2)},
	}
	_, err = db.Exec(`INSERT INTO null_map_test (id, data) VALUES (?, ?)`, 2, regularMap)
	require.NoError(t, err)

	// Verify NULL map
	var id int
	var result *OrderedMap
	err = db.QueryRow(`SELECT id, data FROM null_map_test WHERE id = 1`).Scan(&id, &result)
	require.NoError(t, err)
	require.Equal(t, 1, id)
	require.Nil(t, result)

	// Verify regular map
	var result2 OrderedMap
	err = db.QueryRow(`SELECT id, data FROM null_map_test WHERE id = 2`).Scan(&id, &result2)
	require.NoError(t, err)
	require.Equal(t, 2, id)
	require.Equal(t, regularMap.Keys(), result2.Keys())
	require.Equal(t, regularMap.Values(), result2.Values())
}

func TestGeometry(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	r, err := db.Query(`SELECT 'POINT(1 1)'::GEOMETRY`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, r)

	cols, err := r.ColumnTypes()
	require.NoError(t, err)
	require.Equal(t, "GEOMETRY", cols[0].DatabaseTypeName())
	require.Equal(t, reflectTypeBytes, cols[0].ScanType())

	var res []byte
	require.True(t, r.Next())
	err = r.Scan(&res)
	require.NoError(t, err)
	// We expect Geography/Geometry to come back as a native binary BLOB map
	require.NotEmpty(t, res)
	require.False(t, r.Next())
}
