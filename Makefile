test.examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

test.vendor:
	./scripts/test-vendor.sh

test.dynamic:
	./scripts/test-dynamic.sh

test.static:
	./scripts/test-static.sh

GOBIN ?= $$(go env GOPATH)/bin

install-go-test-coverage:
	go install github.com/vladopajic/go-test-coverage/v2@latest

check-coverage: install-go-test-coverage
	go test ./... -coverprofile=./cover.out -covermode=atomic -coverpkg=./...
	${GOBIN}/go-test-coverage --config=./.github/testcoverage.yml
	go tool cover -html=cover.out -o=cover.html
