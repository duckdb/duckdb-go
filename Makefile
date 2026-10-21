BINDINGS=github.com/duckdb/duckdb-go-bindings
NATIVE_DARWIN_ARM64=go.duckdb.org/libduckdb/darwin-arm64

test.examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

# Download the shared library of the DuckDB build that the bindings pin. ARTIFACT is the
# platform suffix of DuckDB's release artifact, e.g. osx-universal.
test.dynamic.lib:
	base_url=$$(jq -r .base_url "$$(go list -f '{{.Dir}}' ${BINDINGS})/duckdb-release.json") && \
	mkdir dynamic-dir && \
	curl -fsSL "$$base_url/duckdb-shared-libs-${ARTIFACT}.tar.gz" | tar -xz -C dynamic-dir

# Copy the static archives of the bindings' darwin-arm64 module and build their extension loader.
test.static.lib.darwin.arm64:
	native=$$(go list -f '{{.Dir}}' ${NATIVE_DARWIN_ARM64}) && \
	mkdir static-dir && \
	cp "$$native"/*.a static-dir && \
	cc -c "$$native/static_extension_loader.c" -o static-dir/static_extension_loader.o && \
	ar rcs static-dir/libduckdb_static_extension_loader.a static-dir/static_extension_loader.o

GOBIN ?= $$(go env GOPATH)/bin

install-go-test-coverage:
	go install github.com/vladopajic/go-test-coverage/v2@latest

check-coverage: install-go-test-coverage
	go test ./... -coverprofile=./cover.out -covermode=atomic -coverpkg=./...
	${GOBIN}/go-test-coverage --config=./.github/testcoverage.yml
	go tool cover -html=cover.out -o=cover.html
