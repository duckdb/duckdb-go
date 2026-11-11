package ducktype_test

import (
	"math/big"
	"net/netip"
	"testing"

	"github.com/duckdb/duckdb-go/v2/ducktype"
	"github.com/stretchr/testify/require"
)

func TestNullNetAddr(t *testing.T) {
	t.Run("Scan", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan(nil))
			require.False(t, n.Valid)
			require.Equal(t, netip.Addr{}, n.V)
		})

		t.Run("string IPv4", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("192.168.1.1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("192.168.1.1"), n.V)
		})

		t.Run("string IPv6", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("::1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::1"), n.V)
		})

		t.Run("string IPv4 broadcast", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("255.255.255.255"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("255.255.255.255"), n.V)
		})

		t.Run("string IPv4 unspecified", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("0.0.0.0"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("0.0.0.0"), n.V)
		})

		t.Run("string IPv6 unspecified", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("::"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::"), n.V)
		})

		t.Run("string IPv4 loopback", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("127.0.0.1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("127.0.0.1"), n.V)
		})

		t.Run("string IPv4 multicast", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("224.0.0.1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("224.0.0.1"), n.V)
		})

		t.Run("string IPv6 multicast", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("ff02::1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("ff02::1"), n.V)
		})

		t.Run("string IPv4-mapped IPv6", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan("::ffff:192.0.2.1"))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:192.0.2.1"), n.V)
		})

		t.Run("string invalid", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.Error(t, n.Scan("not-an-ip"))
		})

		t.Run("bytes IPv4", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan([]byte{10, 0, 0, 1}))
			require.True(t, n.Valid)
			addr, ok := netip.AddrFromSlice([]byte{10, 0, 0, 1})
			require.True(t, ok)
			require.Equal(t, addr, n.V)
		})

		t.Run("map with string address", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.NoError(t, n.Scan(map[string]any{"address": "10.0.0.2"}))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("10.0.0.2"), n.V)
		})

		t.Run("map with big.Int IPv4", func(t *testing.T) {
			var n ducktype.NullNetAddr
			b := new(big.Int).SetBytes([]byte{192, 168, 0, 1})
			require.NoError(t, n.Scan(map[string]any{"address": b, "ip_type": uint8(1)}))
			require.True(t, n.Valid)
			addr, ok := netip.AddrFromSlice([]byte{192, 168, 0, 1})
			require.True(t, ok)
			require.Equal(t, addr, n.V)
		})

		t.Run("map with big.Int IPv6", func(t *testing.T) {
			var n ducktype.NullNetAddr
			offset := new(big.Int).Lsh(big.NewInt(1), 127)
			b := new(big.Int).SetBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
			b.Sub(b, offset)
			require.NoError(t, n.Scan(map[string]any{"address": b, "ip_type": uint8(2)}))
			require.True(t, n.Valid)
			addr, ok := netip.AddrFromSlice([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
			require.True(t, ok)
			require.Equal(t, addr, n.V)
		})

		t.Run("map with big.Int missing ip_type", func(t *testing.T) {
			var n ducktype.NullNetAddr
			b := new(big.Int).SetBytes([]byte{192, 168, 0, 1})
			require.Error(t, n.Scan(map[string]any{"address": b}))
		})

		t.Run("map with unknown address type", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.Error(t, n.Scan(map[string]any{"address": 12345}))
		})

		t.Run("unsupported type", func(t *testing.T) {
			var n ducktype.NullNetAddr
			require.Error(t, n.Scan(12345))
		})
	})

	t.Run("Value", func(t *testing.T) {
		t.Run("invalid", func(t *testing.T) {
			n := ducktype.NullNetAddr{Valid: false}
			v, err := n.Value()
			require.NoError(t, err)
			require.Nil(t, v)
		})

		t.Run("valid", func(t *testing.T) {
			n := ducktype.NullNetAddr{V: netip.MustParseAddr("1.2.3.4"), Valid: true}
			v, err := n.Value()
			require.NoError(t, err)
			require.Equal(t, "1.2.3.4", v)
		})
	})

	t.Run("select from duckdb", func(t *testing.T) {
		t.Run("string", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '192.168.1.100'::VARCHAR")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("192.168.1.100"), n.V)
		})

		t.Run("null", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT NULL::VARCHAR")
			require.NoError(t, row.Scan(&n))
			require.False(t, n.Valid)
		})

		t.Run("IPv4-mapped IPv6 example 1", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::ffff:5.79.77.51'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:5.79.77.51"), n.V)
		})

		t.Run("IPv4 private address", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '192.168.1.100'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("192.168.1.100"), n.V)
		})

		t.Run("IPv4 loopback", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '127.0.0.1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("127.0.0.1"), n.V)
		})

		t.Run("IPv4 unspecified", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '0.0.0.0'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("0.0.0.0"), n.V)
		})

		t.Run("IPv4 broadcast", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '255.255.255.255'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("255.255.255.255"), n.V)
		})

		t.Run("IPv4 multicast", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '224.0.0.1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("224.0.0.1"), n.V)
		})

		t.Run("IPv6 loopback", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::1"), n.V)
		})

		t.Run("IPv6 unspecified", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::"), n.V)
		})

		t.Run("IPv6 multicast", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT 'ff02::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("ff02::1"), n.V)
		})

		t.Run("IPv6 link-local", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT 'fe80::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("fe80::1"), n.V)
		})

		t.Run("IPv6 unique local", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT 'fc00::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("fc00::1"), n.V)
		})

		t.Run("IPv6 documentation prefix", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '2001:db8::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("2001:db8::1"), n.V)
		})

		t.Run("IPv6 max value", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), n.V)
		})

		t.Run("IPv6 boundary 8000::", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '8000::'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("8000::"), n.V)
		})

		t.Run("IPv6 boundary 8000::1", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '8000::1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("8000::1"), n.V)
		})

		t.Run("IPv6 boundary 7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("7fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), n.V)
		})

		t.Run("IPv4-mapped IPv6 zero", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::ffff:0.0.0.0'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:0.0.0.0"), n.V)
		})

		t.Run("IPv4-mapped IPv6 broadcast", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::ffff:255.255.255.255'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:255.255.255.255"), n.V)
		})

		t.Run("IPv4-mapped IPv6 loopback", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '::ffff:127.0.0.1'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:127.0.0.1"), n.V)
		})

		t.Run("IPv4 CIDR network address", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '192.168.1.0/24'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("192.168.1.0"), n.V)
		})

		t.Run("IPv6 CIDR network address", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT '2001:db8::/32'::INET")
			require.NoError(t, row.Scan(&n))
			require.True(t, n.Valid)
			require.Equal(t, netip.MustParseAddr("2001:db8::"), n.V)
		})

		t.Run("null via INET", func(t *testing.T) {
			db := newDB(t)
			var n ducktype.NullNetAddr
			row := db.QueryRowContext(t.Context(), "SELECT NULL::INET")
			require.NoError(t, row.Scan(&n))
			require.False(t, n.Valid)
		})

		t.Run("roundtrip IPv4 through INET column", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE inet_test_ipv4 (v INET)")
			require.NoError(t, err)

			in := ducktype.NullNetAddr{V: netip.MustParseAddr("192.168.1.100"), Valid: true}
			_, err = db.ExecContext(ctx, "INSERT INTO inet_test_ipv4 VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullNetAddr
			row := db.QueryRowContext(ctx, "SELECT v FROM inet_test_ipv4")
			require.NoError(t, row.Scan(&out))
			require.True(t, out.Valid)
			require.Equal(t, netip.MustParseAddr("192.168.1.100"), out.V)
		})

		t.Run("roundtrip IPv6 through INET column", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE inet_test_ipv6 (v INET)")
			require.NoError(t, err)

			in := ducktype.NullNetAddr{V: netip.MustParseAddr("2001:db8::1"), Valid: true}
			_, err = db.ExecContext(ctx, "INSERT INTO inet_test_ipv6 VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullNetAddr
			row := db.QueryRowContext(ctx, "SELECT v FROM inet_test_ipv6")
			require.NoError(t, row.Scan(&out))
			require.True(t, out.Valid)
			require.Equal(t, netip.MustParseAddr("2001:db8::1"), out.V)
		})

		t.Run("roundtrip IPv4-mapped IPv6 through INET column", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE inet_test_mapped (v INET)")
			require.NoError(t, err)

			in := ducktype.NullNetAddr{V: netip.MustParseAddr("::ffff:5.79.77.51"), Valid: true}
			_, err = db.ExecContext(ctx, "INSERT INTO inet_test_mapped VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullNetAddr
			row := db.QueryRowContext(ctx, "SELECT v FROM inet_test_mapped")
			require.NoError(t, row.Scan(&out))
			require.True(t, out.Valid)
			require.Equal(t, netip.MustParseAddr("::ffff:5.79.77.51"), out.V)
		})

		t.Run("roundtrip null through INET column", func(t *testing.T) {
			db := newDB(t)
			ctx := t.Context()

			_, err := db.ExecContext(ctx, "CREATE TABLE inet_test_null (v INET)")
			require.NoError(t, err)

			in := ducktype.NullNetAddr{Valid: false}
			_, err = db.ExecContext(ctx, "INSERT INTO inet_test_null VALUES (?)", in)
			require.NoError(t, err)

			var out ducktype.NullNetAddr
			row := db.QueryRowContext(ctx, "SELECT v FROM inet_test_null")
			require.NoError(t, row.Scan(&out))
			require.False(t, out.Valid)
		})
	})
}
