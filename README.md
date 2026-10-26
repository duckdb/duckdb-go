# Go SQL Driver For [DuckDB](https://github.com/duckdb/duckdb)

![Tests status](https://github.com/duckdb/duckdb-go/actions/workflows/tests.yaml/badge.svg)
[![GoDoc](https://godoc.org/github.com/duckdb/duckdb-go/v2?status.svg)](https://pkg.go.dev/github.com/duckdb/duckdb-go/v2)

The DuckDB driver conforms to the built-in `database/sql` interface.

**Current DuckDB version: `v2.0.0`.**

The first duckdb-go tag with that version is `v2.20000.0`.

Starting with DuckDB `v1.5.0`, the duckdb-go version encodes the DuckDB version in its second semver component.
The format is `v2.MAJOR_MINOR_PATCH.x`, e.g., DuckDB `v1.5.0` maps to duckdb-go `v2.10500.x`.

Previous DuckDB versions:

| DuckDB   | duckdb-go    |
|----------|--------------|
| `v2.0.0` | `v2.20000.0` |
| `v1.5.6` | `v2.10506.0` |
| `v1.5.5` | `v2.10505.0` |
| `v1.5.4` | `v2.10504.0` |
| `v1.5.3` | `v2.10503.1` |
| `v1.5.2` | `v2.10502.0` |
| `v1.5.1` | `v2.10501.0` |
| `v1.5.0` | `v2.10500.0` |
| `v1.4.5` | `v2.5.6`     |
| `v1.4.4` | `v2.5.5`     |
| `v1.4.3` | `v2.5.4`     |
| `v1.4.2` | `v2.5.2`     |
| `v1.4.1` | `v2.4.2`     |
| `v1.4.0` | `v2.4.0`     |
| `v1.3.2` | `v2.3.3`     |
| `v1.3.1` | `v2.3.2`     |
| `v1.3.0` | `v2.3.0`     |
| `v1.2.2` | `v2.2.0`     |
| `v1.2.1` | `v2.1.0`     |
| `v1.2.0` | `v2.0.3`     |
| `v1.1.3` | `v1.8.5`     |

## Migration from marcboeker/go-duckdb

**This project moved from `github.com/marcboeker/go-duckdb` to `github.com/duckdb/duckdb-go` starting with `v2.5.0`.**

All versions prior to `v2.5.0` use the old import paths.

To migrate:

```sh
# Update dependency
go get github.com/duckdb/duckdb-go/v2@v2.5.0

# Rewrite import paths
gofmt -w -r '"github.com/marcboeker/go-duckdb/v2" -> "github.com/duckdb/duckdb-go/v2"' .

# If you use the mapping or arrowmapping submodules, also run
gofmt -w -r '"github.com/marcboeker/go-duckdb/mapping" -> "github.com/duckdb/duckdb-go/v2/mapping"' .
gofmt -w -r '"github.com/marcboeker/go-duckdb/arrowmapping" -> "github.com/duckdb/duckdb-go/v2/arrowmapping"' .

# Clean up
go mod tidy
```

### Background

Over the last few years, the Go client has become a [primary DuckDB client](https://duckdb.org/docs/stable/clients/overview).
We'd like to thank [Marc Boeker](https://github.com/marcboeker) for all his work on creating this driver and implementing the various interfaces of the `database/sql` package!
We'd also like to thank all the other external contributors for their various PRs and other contributions.

With the driver being a primary DuckDB client, over the last years, the DuckDB team has gradually increased its involvement in the maintenance of the driver,
to guarantee that it is constantly updated and that critical bugs (e.g., crashes) are fixed.
Now we have a Long-Term Support release, so starting from early next year, we will have two releases in parallel (v1.4 LTS and v1.5).
Additionally, more DuckDB customers use and rely on the Go client, which necessitates prioritization of certain features from us.

These points all add to the maintenance work, which the DuckDB team is happy to perform!
However, the motivation behind this fork, which is a joint effort of Marc Boeker and the DuckDB team,
is to fully transfer the maintenance and day-to-day work of the driver to the DuckDB team.
That being said, the DuckDB Go client has become what it is also due to its many contributions from the community,
and we are looking forward to your future PRs, issues, and discussions!

The license is unchanged: the migrated repository keeps the original MIT license, which is the same for core DuckDB and other primary clients.

## Upgrading to DuckDB v2

DuckDB `v2.0.0` maps to duckdb-go `v2.20000.x`. The `/v2` in duckdb-go's module path is unrelated to DuckDB's major version.
duckdb-go keeps its Go API, so existing code builds unchanged.
What else changes depends on how you link DuckDB and which features you use.
See also the [duckdb-go-bindings upgrade notes](https://github.com/duckdb/duckdb-go-bindings#upgrading-to-duckdb-v2).

### Linking

- **Pre-built static libraries (default):** nothing to do.
  The libraries now come from `go.duckdb.org` instead of GitHub,
  so builds that restrict module downloads, e.g., with `GOPROXY` or `GOPRIVATE`, must allow it.
- **Dynamic libraries (`duckdb_use_lib`):** link the shared library of a DuckDB v2 release,
  published as `duckdb-shared-libs-<platform>.tar.gz` (see [Linking a Dynamic Library](#linking-a-dynamic-library)).
- **Custom static libraries (`duckdb_use_static_lib`):** DuckDB v2 removed the `bundle-library` target.
  Link a static extension loader and the v2 set of archives instead (see [Linking a Static Library](#linking-a-static-library)).

### Behavior changes

- `RowsAffected` of a `CREATE TABLE ... AS` statement returns the number of created rows instead of 0.
- Profiling metrics and configuration have changed; see [DuckDB Profiling API](#duckdb-profiling-api).
- Setting an option with `PRAGMA name = value` is deprecated and logs a warning. Use `SET name = value` instead.
- With DuckDB `v2.0.0`, a custom log storage (`RegisterLogStorage`) receives each query twice: once when duckdb-go prepares it, and once when it executes it.
- A failed automatic flush, `Flush`, or `FlushWithCancel` drops the rows it tried to append instead of keeping them for the next flush.
  An `AppendRow` call that returns such an error does not append its row either.
  `Close` still returns an error for that flush unless you call `Clear` first.
  Batches appended after the failure can still commit, so that error does not imply that no rows were written.

## Breaking Changes

> [!WARNING]
> duckdb-go `v2.0.0`, the first release of the `v2` module, shipped with DuckDB `v1.2.0`.
> Moving from duckdb-go `v1` to `v2` includes the following list of breaking changes.
> Later `v2` releases add to the list; those entries say so.

#### Dropping pre-built FreeBSD support

Starting with `v2`, duckdb-go drops pre-built FreeBSD support.
This change is because DuckDB does not publish any bundled FreeBSD libraries.
Thus, you must build your static library for FreeBSD using the steps below.

#### The Arrow dependency is now opt-in

The [DuckDB Arrow Interface](https://duckdb.org/docs/api/c/api#arrow-interface) is a heavy dependency.
Starting with `v2`, the DuckDB Arrow Interface is opt-in instead of opt-out.
If you want to use it, you can enable it by passing `-tags=duckdb_arrow` to `go build`.

#### JSON type scanning changes

The pre-built libraries ship DuckDB's JSON extension containing the `JSON` type.
Pre-v2, it was possible to scan a JSON type into `[]byte` via [`Rows.Scan`](https://pkg.go.dev/database/sql#Rows.Scan).
However, scanning into `any` (`driver.Value`) would cause the JSON string to contain escape characters and other unexpected behavior.

It is now possible to scan into `any`, or directly into duckdb-go's `Composite` type,
as shown in the [JSON example](https://github.com/duckdb/duckdb-go/blob/main/examples/json/main.go).
Scanning directly into `string` or `[]byte` is no longer possible.
A workaround is casting to `::VARCHAR` or `::BLOB` in DuckDB if you do not need to scan the result into a JSON interface.

#### JSON type writing changes

This change landed after `v2.0.0`, so it also affects upgrades between `v2` releases.

`SetChunkValue` and `SetRowValue` previously wrote to a `JSON` column by storing the value verbatim,
because DuckDB stores `JSON` as `VARCHAR` and the two were indistinguishable on the write path.
They now marshal the value with `encoding/json`, matching what `DataChunk.SetValue` has always done.

This changes what a `string` or a `[]byte` writes: a `string` becomes a JSON string (`hello` is stored as `"hello"`),
and a `[]byte` becomes a base64-encoded JSON string. Use `json.RawMessage` to write a pre-serialized JSON document.

## Installation

```sh
go get github.com/duckdb/duckdb-go/v2
```

### Windows

You must have the correct version of gcc and the necessary runtime libraries installed on Windows.
One method to do this is using msys64.
To begin, install msys64 using their installer.
Once you installed msys64, open a msys64 shell and run:

```sh
pacman -S mingw-w64-ucrt-x86_64-gcc
```

Select "yes" when necessary; it is okay if the shell closes.
Then, add gcc to the path using whatever method you prefer.
In powershell this is `$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"`.
After, you can compile this package in Windows.

### Vendoring

You can use `go mod vendor` to make a copy of the third-party packages in this package, including the pre-built DuckDB libraries in [duckdb-go-bindings](https://github.com/duckdb/duckdb-go-bindings).

Run `make test.vendor` to vendor an external consumer of the checkout and execute a query offline with empty caches.
This catches missing headers and native libraries; it runs in Linux amd64 CI and does not validate published module archives.

## Usage

_Note: For readability, we omit error handling in most examples._

`duckdb-go` hooks into the `database/sql` interface provided by the Go `stdlib`.
To open a connection, specify the driver type as `duckdb`.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()
```

The above lines create an in-memory instance of DuckDB.
To open a persistent database, specify a file path to the database file.
If the file does not exist, then DuckDB creates it.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db")
defer db.Close()
```

If you want to set specific [config options for DuckDB](https://duckdb.org/docs/sql/configuration),
you can add them as query style parameters in the form of `name=value` pairs to the DSN.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db?access_mode=read_only&threads=4")
defer db.Close()
```

Alternatively, you can use [sql.OpenDB](https://cs.opensource.google/go/go/+/refs/tags/go1.23.0:src/database/sql/sql.go;l=824).
That way, you can perform initialization steps in a callback function before opening the database.
Here's an example that configures some parameters when opening a database with `sql.OpenDB(connector)`.

```go
c, err := duckdb.NewConnector("/path/to/foo.db?access_mode=read_only&threads=4", func(execer driver.ExecerContext) error {
    bootQueries := []string{
        `SET schema=main`,
        `SET search_path=main`,
    }
    for _, query := range bootQueries {
        _, err = execer.ExecContext(context.Background(), query, nil)
        if err != nil {
            return err
        }
    }
    return nil
})
defer c.Close()
db := sql.OpenDB(c)
defer db.Close()
```

Please refer to the [database/sql](https://godoc.org/database/sql) documentation for further instructions on usage.

## Linking DuckDB

By default, `duckdb-go` statically links pre-built DuckDB libraries into your binary.
Statically linking DuckDB increases your binary size.

On Linux, this does not make the executable fully static: it still depends on system libraries, including glibc.
Use compatible build and runtime images, preferably the same distribution release.
Avoid forcing static glibc linking with `-static`: runtime library loading can fail with an incompatible glibc.

`duckdb-go` bundles the following pre-compiled static libraries.

- MacOS: amd64, arm64.
- Linux: amd64, arm64.
- Windows: amd64.

### Linking a Static Library

If none of the pre-built libraries satisfy your needs, you can link your own DuckDB static libraries
by passing `-tags=duckdb_use_static_lib` to `go build`.

DuckDB releases publish them as `duckdb-static-libs-<platform>.tar.gz`.
DuckDB's install script fetches them for the current platform into `~/.duckdb/lib/<version>/release_<platform>`:

```sh
curl https://install.duckdb.org | DUCKDB_INSTALL=static sh
```

DuckDB only loads statically linked extensions once they are registered.
duckdb-go calls `duckdb_register_static_extensions()` before opening a database,
so you must also link a static extension loader that defines it.
When you build DuckDB from source, `make` leaves the archives under `build/release`,
and `make static_extension_loader` generates the loader for that build.
Without a DuckDB checkout, start from the loader of the pre-built libraries, as described in the
[duckdb-go-bindings README](https://github.com/duckdb/duckdb-go-bindings#alternative-custom-static-libraries).

Link the loader, an archive for each capability and extension it registers, and `libduckdb_static.a` last.
For Darwin ARM64, with the extensions of the pre-built libraries:

```sh
CGO_ENABLED=1 CPPFLAGS="-DDUCKDB_STATIC_BUILD" \
CGO_LDFLAGS="-lduckdb_static_extension_loader -lduckdb_httplib -lduckdb_loadable_extensions -lautocomplete_extension -lcore_functions_extension -licu_extension -ljson_extension -lparquet_extension -ltpcds_extension -ltpch_extension -lduckdb_static -lc++ -L/path/to/libs" \
go build -tags=duckdb_use_static_lib
```

The duckdb-go-bindings README lists the linker flags for Linux and Windows.

#### Note on FreeBSD

Starting with `v2`, duckdb-go drops pre-built FreeBSD support.
This change is because DuckDB does not publish any bundled FreeBSD libraries.
Thus, you must build your static library for FreeBSD using the steps above.

### Linking a Dynamic Library

Alternatively, you can dynamically link DuckDB by passing `-tags=duckdb_use_lib` to `go build`.
You must have a copy of `libduckdb` available on your system (`.so` on Linux or `.dylib` on macOS).
DuckDB releases publish it as `duckdb-shared-libs-<platform>.tar.gz`,
and `curl https://install.duckdb.org | DUCKDB_INSTALL=shared sh` installs it into `~/.duckdb/lib/<version>/release_<platform>`.

For example:

```sh
# On Linux.
CGO_ENABLED=1 CGO_LDFLAGS="-lduckdb -L/path/to/libs" go build -tags=duckdb_use_lib main.go
LD_LIBRARY_PATH=/path/to/libs ./main

# On MacOS.
CGO_ENABLED=1 CGO_LDFLAGS="-lduckdb -L/path/to/libs" go build -tags=duckdb_use_lib main.go
DYLD_LIBRARY_PATH=/path/to/libs ./main
```

You can also find these steps in the `Makefile` and the `tests.yaml`.

#### Using a local DuckDB checkout

To validate local DuckDB changes from `duckdb-go`, build DuckDB and link `duckdb-go` against its shared library.
Rerun `make` after editing DuckDB source files.

```sh
cd /path/to/duckdb
make

cd /path/to/duckdb-go
CGO_ENABLED=1 \
CGO_LDFLAGS="-lduckdb -L/path/to/duckdb/build/release/src -Wl,-rpath,/path/to/duckdb/build/release/src" \
go test -tags=duckdb_use_lib ./...
```

To run only the test you care about, add `-run TestName -v`.

## Notes and FAQs

**`undefined: conn`**

Some people encounter an `undefined: conn` error when building this package.
This error is due to the Go compiler determining that CGO is unavailable.
This error can happen due to a few issues.

The first cause, as noted in the [comment here](https://github.com/marcboeker/go-duckdb/issues/275#issuecomment-2355712997),
might be that the `buildtools` are not installed.
To fix this for ubuntu, you can install them using:

```
sudo apt-get update && sudo apt-get install build-essential
```

Another cause can be cross-compilation since the Go compiler automatically disables CGO when cross-compiling.
To enable CGO when cross-compiling, use `CC={C cross compiler} CGO_ENABLED=1 {command}` to force-enable CGO and set the right cross-compiler.

**`TIMESTAMP vs. TIMESTAMP_TZ`**

In the C API, DuckDB stores both `TIMESTAMP` and `TIMESTAMP_TZ` as `duckdb_timestamp`, which holds the number of
microseconds elapsed since January 1, 1970, UTC (i.e., an instant without offset information).
When passing a `time.Time` to duckdb-go, duckdb-go transforms it to an instant with `UnixMicro()`,
even when using `TIMESTAMP_TZ`. Later, scanning either type of value returns an instant, as SQL types do not model
time zone information for individual values.

Use `duckdb.Typed(value, typ)` to force the DuckDB logical type used when binding a query parameter.
This is useful when DuckDB cannot infer the desired parameter type from SQL alone, or when duckdb-go's
default Go-type inference would choose a different DuckDB type.

```go
start := time.Date(2024, time.April, 5, 0, 0, 0, 0, time.UTC)
end := time.Date(2024, time.April, 6, 0, 0, 0, 0, time.UTC)

row := db.QueryRow(`
	SELECT COUNT(*)
	FROM (VALUES
		(TIMESTAMP_NS '2024-04-05 12:00:00.000000001')
	) events_ns(ts)
	WHERE ts >= ? AND ts < ?
`, duckdb.Typed(start, duckdb.TYPE_TIMESTAMP_NS), duckdb.Typed(end, duckdb.TYPE_TIMESTAMP_NS))
```

In this example, the wrapper ensures the parameters bind as `TIMESTAMP_NS`. Bare `time.Time` values bind as
`TIMESTAMP_TZ` by default. `Typed` is a narrow scalar binding hint; validation happens when the parameter is bound.

Unlike timestamps, `DATE` and `TIME` use the calendar date and wall clock in the `time.Time` value's own location:
`2024-01-15 08:30:00+09:00` becomes `DATE '2024-01-15'` and `TIME '08:30:00'`.
Call `value.UTC()` first to get the UTC date and clock.
Query parameters must bind as `DATE` or `TIME` for this: a bare `time.Time` in `?::DATE` binds as
`TIMESTAMP_TZ`, so the cast uses the session `TimeZone`; use `Typed(value, TYPE_DATE)` instead.

**Connection lifetime**

Temporary objects and state, such as temporary tables, are scoped to connections.
When closing a connection, Go's `database.sql` pooling logic might cache it as an idle connection,
instead of invoking its clean-up code by closing the connection.
That behavior can lead to, e.g., temporary tables persisting longer than expected.
To disable keeping idle connections alive, use `db.SetMaxIdleConns(0)`.

## Memory Allocation

DuckDB lives in process.
Therefore, all its memory lives in the driver.
All allocations live in the host process, which is the Go application.
Especially for long-running applications, it is crucial to call the corresponding `Close`-functions as specified in [database/sql](https://godoc.org/database/sql).

Additionally, it is crucial to call `Close()` on the database and/or connector of a persistent DuckDB database.
That way, DuckDB synchronizes all changes from the WAL to its persistent storage.

The following is a list of examples of `Close()`-functions.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

rows, err := conn.QueryContext(context.Background(), "SELECT 42")
// Alternatively, rows.Next() has to return false.
rows.Close()

appender, err := duckdb.NewAppenderFromConn(conn, "", "test")
defer appender.Close()

c, err := duckdb.NewConnector("", nil)
// Optional, if passed to sql.OpenDB.
defer c.Close()
```

## DuckDB Appender API

If you want to use the [DuckDB Appender API](https://duckdb.org/docs/data/appender.html), you can obtain a new `Appender` by passing a DuckDB connection to `NewAppenderFromConn()`.
See `examples/appender/main.go` for a complete example.

```go
c, err := duckdb.NewConnector("test.db", nil)
defer c.Close()

conn, err := c.Connect(context.Background())
defer conn.Close()

// Obtain an appender from the connection.
// NOTE: The table 'test_tbl' must exist in test.db.
appender, err := NewAppenderFromConn(conn, "", "test_tbl")
defer appender.Close()

err = appender.AppendRow(...)
```

## DuckDB Profiling API

This section describes using the [DuckDB Profiling API](https://duckdb.org/docs/dev/profiling.html).
DuckDB's profiling information is connection-local.
The following example walks you through the necessary steps to obtain the `ProfilingInfo` type, which contains all available metrics.
Please refer to the [DuckDB documentation](https://duckdb.org/docs/dev/profiling.html) on configuring and collecting specific metrics.

- First, you need to obtain a connection.
- Then, you enable profiling for the connection.
- Now, for each subsequent query on this connection, DuckDB will collect profiling information.
  - Optionally, you can turn off profiling at any point.
- Next, you execute the query for which you want to obtain profiling information.
- Finally, directly after executing the query, retrieve any available profiling information.

```Go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

_, err = conn.ExecContext(context.Background(), `SET enable_profiling = 'no_output'`)

res, err := conn.QueryContext(context.Background(), `SELECT 42`)
defer res.Close()

info, err := GetProfilingInfo(conn)

_, err = conn.ExecContext(context.Background(), `PRAGMA disable_profiling`)
```

The root of the `ProfilingInfo` tree has no metrics of its own.
Its children are the operators of the query plan, followed by metric groups measured over the entire query,
such as the group containing the query's `sql` and `cpu_time`.
Metric names are lowercase, e.g., `cpu_time` instead of `CPU_TIME`.
`SET tracked_metrics` selects the collected metrics, e.g., `SET tracked_metrics = ['query.cpu_time', 'storage.checkpoint_latency']`.
It replaces `custom_profiling_settings`; `profiling_mode` is deprecated.

## DuckDB Apache Arrow Interface

The [DuckDB Arrow Interface](https://duckdb.org/docs/api/c/api#arrow-interface) is a heavy dependency.
Starting with `v2`, the DuckDB Arrow Interface is opt-in instead of opt-out.
If you want to use it, you can enable it by passing `-tags=duckdb_arrow` to `go build`.

```sh
go build -tags="duckdb_arrow"
```

You can obtain a new `Arrow` by passing a DuckDB connection to `NewArrowFromConn()`.

```go
c, err := duckdb.NewConnector("", nil)
defer c.Close()

conn, err := c.Connect(context.Background())
defer conn.Close()

// Obtain the Arrow from the connection.
arrow, err := duckdb.NewArrowFromConn(conn)

rdr, err := arrow.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10)")
defer rdr.Release()

for rdr.Next() {
  // Process each record.
}
```

> [!WARNING]  
> Arrow connections are not safe for concurrent use, and do not benefit from `database/sql` connection pooling.

## DuckDB Extensions

`duckdb-go` relies on the [`duckdb-go-bindings` module](https://github.com/duckdb/duckdb-go-bindings).
Any pre-built library in `duckdb-go-bindings` statically links the core functions and the Autocomplete, ICU, JSON, Parquet, TPC-DS, and TPC-H extensions.
Additionally, automatic extension loading is enabled.

## Releasing a New DuckDB Version

Prepare normal releases from `main`. For LTS releases that stay on DuckDB 1.4 Andium, use the `v1.4-andium` branch.

1. Release the new DuckDB version in [duckdb-go-bindings](https://github.com/duckdb/duckdb-go-bindings#updating-native-libraries).
2. Create a new branch.
3. Update `duckdb-go-bindings` via `go get github.com/duckdb/duckdb-go-bindings@latest`.
4. Run `go mod tidy`.
5. Update the latest version and the version table in `README.md`.
6. Commit and PR changes. Link the bindings release in the PR description.
7. After the PR is merged, push a new tagged release, `v2.MAJOR_MINOR_PATCH.x`, e.g. `v2.10500.0` for DuckDB 1.5.0, and publish a GitHub release for the tag.

```
git tag <tagname>
git push origin <tagname>
```
