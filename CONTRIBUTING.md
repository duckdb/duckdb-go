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

#### CI test scope

Every pull request, push to `main`, and tag push executes the applicable suites.
Main tests cover Linux x64 and ARM64, macOS ARM64, and Windows on pull requests.
Pushes and manual runs also cover macOS Intel main tests and dynamic linking.
Additional suites cover Linux x64 race tests and coverage, Arrow
on Linux x64, macOS ARM64, and Windows, examples on Linux x64 and macOS ARM64,
dynamic linking on Linux and macOS, and explicit static linking on macOS ARM64.

For complete manual validation, dispatch both the Tests and Tidy Checks
workflows on the same branch or tag. Tests runs the five platform jobs;
Tidy Checks runs lint separately.

Test suites share one runner per platform to reduce queueing; a failed suite
does not prevent the remaining suites from running. With a separate lint job,
CI uses five runners per pull request and six on pushes and manual runs.
Feature-branch pushes do not trigger duplicate
checks alongside pull requests. No labels or changed-file classification are
needed to run the full suite.

Post-merge failures should be repaired or reverted promptly. Before releasing,
ensure the full suite passes on the exact commit being released.

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
