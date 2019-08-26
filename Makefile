export CGO_ENABLED=0
export GO111MODULE=on

.PHONY: build

all: build

build: # @HELP build the source code
build:
	go build -v ./...

test: # @HELP run the unit tests and source code validation
test: build license_check linters
	go test github.com/atomix/atomix-go-client/pkg/...

coverage: # @HELP generate unit test coverage data
coverage: build linters license_check
	./build/bin/coveralls-coverage

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run --exclude-use-default=false

license_check: # @HELP examine and ensure license headers exist
	./build/licensing/boilerplate.py -v
