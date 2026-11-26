# Contributing

### Contributions

* Are you fixing a bug or adding a new feature - Great!
* Announce that you are working on an issue by replying to it. 
Open a new issue to announce that you are working on a new feature.
* Try to avoid large pull requests - they are much harder to review. 

### Pull Requests

* Ensure the PR description clearly describes the problem and solution.
Include the relevant issue number if applicable.
* Add tests to cover your fix or new functionality.
* Pull requests need to pass all continuous integration checks before merging.
* We reserve full and final discretion over whether we merge a pull request or not. 
Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

### Testing

`duckdb-go` aims to contain tests that are fast, cover a significant portion of the code, 
are comprehensible, and can serve as documentation.

On Linux, macOS, or Windows AMD64, run the tests against the libraries that the bindings pin
for the current platform:

```sh
make test.dynamic
make test.static
```

On Windows AMD64, use Git Bash with `make` and the [UCRT compiler setup](README.md#amd64) on `PATH`.
Both linking modes require `cygpath` (included with Git Bash) for native Windows paths.
The dynamic tests add the DLL directory to `PATH` for the test executable.

To include Arrow tests, as the custom-linking CI jobs do:

```sh
TAGS=duckdb_arrow make test.dynamic
TAGS=duckdb_arrow make test.static
```

The targets run `scripts/test-dynamic.sh` and `scripts/test-static.sh` against `./...`.
To select packages or pass `go test` arguments, invoke either script directly, for example:

```sh
./scripts/test-dynamic.sh . -run TestOpen -v
./scripts/test-static.sh ./internal/capi2/... -v
```

With flags but no package, the scripts test only the root package, so pass `./...` explicitly to test all packages.
Do not pass `-tags` or `-ldflags`; they replace the scripts' library and linker settings.

Both targets require `jq` and either `sha256sum` or `shasum`.
`test.dynamic` also requires `curl` and `tar`.
It downloads and verifies the shared library before running the tests, caching the archive
in `dynamic-dir` for subsequent runs, including offline runs.
`test.static` uses the native module's static libraries.

#### Coverage

Run `make check-coverage`, then open `cover.html` in a browser.
For a terminal summary, run `go tool cover -func=cover.out`.

CI reports are available in completed
[Tidy Checks runs](https://github.com/duckdb/duckdb-go/actions/workflows/tidy_checks.yml):
download the `Cover_html` artifact, extract the ZIP, and open `cover.html`.

### Go Guidelines

duckdb-go uses [Effective Go](https://go.dev/doc/effective_go) and the 
[Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) 
for tips for writing clear, idiomatic Go code. Below are a few excerpts.

* **Error Naming.**
For error values stored as global variables, use the prefix `Err` or `err`. 
For custom error types, use the suffix `Error` instead.
* **Commentary.**
Comments for humans are formatted like so: `// Uppercase sentence. Single-space next sentence.`
* **Lines.** 
Avoid overly long lines. You can aim to wrap lines that exceed a soft line 
length limit of 100 characters.
* **Naming.**
Choose descriptive, concise names. Avoid single-letter variable names.
* Avoid unnecessary interfaces, only export relevant functions.

## Upgrading DuckDB

See [Releasing a New DuckDB Version](README.md#releasing-a-new-duckdb-version) in the README.
