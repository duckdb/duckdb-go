# Pinned native bootstrap

This is an internal experimental C API v2 surface, not a second driver.
The existing `duckdb` driver, mapping handles, and duckdb-go-bindings remain on v1.

## Pin and initialization

`manifest.json` records DuckDB v2.0.1, its exact source revision, and the SHA256
of both unmodified upstream headers in `include/`.
The required calls include unstable declarations: engine version 2, C API v2,
and this Go module's `/v2` suffix are independent version choices.
Re-audit signatures and ownership on each engine update.

Importing this package makes no DuckDB calls.
Call `Bootstrap` explicitly before creating an environment.
It checks the linked engine using the stable `duckdb_library_version` signature
before calling any v2 function or registering static extensions.
Concurrent/repeated calls share one cached result, including failures.
Prebuilt and custom-static builds invoke the loader supplied by the existing
native modules or static-library setup; shared libraries register themselves.
The bootstrap instances disable extension autoload and autoinstall.

The version check is necessary, not sufficient, for arbitrary custom libraries.
Use the exact manifest source revision and headers, retain build/artifact
provenance, and verify their hashes before linking.
A custom engine reporting `v2.0.1` is not automatically source/ABI compatible.
Go module minimum-version selection can also select a newer native artifact.
`scripts/check-capi2-native.sh` checks the selected prebuilt/static provenance,
or both headers from the checksummed shared archive, against this pin.
A library missing referenced symbols may fail to load before the guard runs,
especially with eager symbol binding.

## Ownership and coexistence

All exposed handles are owned and must not be copied.
Close chunks, results, statements/iterators, connections, instances, and
environments explicitly; borrowed vectors never escape their chunks.
Every native status is checked and caller-owned error information is copied and
destroyed. Each call uses a fresh error slot, since successful calls do not clear it.
`Result.Close` aborts unfinished work. `Drain` completes side effects and reports
deferred errors; fetching to end-of-stream completes row-producing queries.
Per-cell reads are a correctness bridge, not the intended vectorized hot path.

V1 and v2 may coexist on separate files or separate in-memory databases.
Concurrent mixed-API access to one live file is unsupported.
There is no handle conversion or shared backend.
One v2 environment detects conflicting file attachments but does not cache
instances. Other environments do not coordinate file access.
Successful environment destruction is not a connection-leak check.
The persistence test requires same-environment attachment to fail while a
connection survives (`RESOURCE_IN_USE` or `IO_GENERAL`), then succeed after
releasing it, in addition to reading the persisted value after full reopen.

## Validation boundary

The existing Go 1.24 platform/linking matrix now includes the internal tests in
prebuilt, static, and dynamic modes, with pointer checking and existing race
coverage. PRs still run only the repository's fast platform subset.
Configured jobs are not completed cross-platform validation.

The Linux amd64 dynamic job runs `scripts/test-capi2-mismatch.sh`: the same
executable first passes persistence against 2.0.1, then rejects a checksummed
real 2.0.0 library with eager symbol binding, without initializing v1 Go bindings.
Static tests also run an isolated identical/conflicting-registration control.
The C shims compile with the upstream host-table macros, but no Go loadable
extension, experimental driver, appender/Arrow port, or native publishing move
is implemented here. Those remain later deliverables.
