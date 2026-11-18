# Cross-Compilation

`duckdb-go` uses CGO to link a native DuckDB library.
Cross-compilation requires a C/C++ toolchain and a compatible DuckDB library for the target.
The default pre-built library is selected automatically by the target `GOOS` and `GOARCH`.
See [Linking DuckDB](../README.md#linking-duckdb) for available platforms, custom libraries, and Linux runtime requirements.

## Build your application

In your application's `package main` directory, run the recipe for your host and target:

```sh
# macOS arm64 -> darwin/amd64 (xcode-select --install)
CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o app .

# macOS arm64 -> linux/arm64
brew install messense/macos-cross-toolchains/aarch64-unknown-linux-gnu
CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ \
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build -o app .

# macOS arm64 -> linux/amd64
brew install messense/macos-cross-toolchains/x86_64-unknown-linux-gnu
CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ \
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o app .

# Debian 13 amd64 -> windows/amd64
sudo apt-get install g++-mingw-w64-ucrt64
CC=x86_64-w64-mingw32ucrt-gcc CXX=x86_64-w64-mingw32ucrt-g++ \
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 go build -o app.exe .
```

Run the executable and a query in the target environment to verify runtime compatibility.

Last verified with bindings `v0.20000.0`, Apple clang 17, messense GCC 15.2, and Debian 13 UCRT GCC 14.
Consumer applications queried DuckDB successfully under Rosetta, in Debian 13 containers, and under Wine 10, respectively.

## Compatibility considerations

- **Older Linux runtimes:** the tested messense GCC 15.2 linux/amd64 build requires `GLIBCXX_3.4.32`.
  It runs on Debian 13 but fails on Debian 12's stock `libstdc++` with ``version `GLIBCXX_3.4.32' not found``.
  Build in a container matching the target runtime if the cross-toolchain requires newer system libraries.
- **Windows CRT:** the pre-built Windows library requires UCRT.
  MSVCRT-based toolchains can fail with unresolved symbols such as `__stdio_common_vsnprintf_s`.
  The UCRT package above requires Debian 13; see the [MinGW-w64 Debian instructions](https://www.mingw-w64.org/getting-started/debian/).
- **C++ runtime compatibility:** Linux and Windows prebuilts use `libstdc++`, while Darwin prebuilts use `libc++`.
  The target toolchain must be compatible with the library's C++ ABI and runtime.
  The tested Zig 0.17.0 linux/amd64 configuration fails to link the prebuilts with undefined `std::__cxx11` symbols.

Other host/target pairs may work; see [issue #20](https://github.com/duckdb/duckdb-go/issues/20).
