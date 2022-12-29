# SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
#
# SPDX-License-Identifier: Apache-2.0

.PHONY: build

all: build

build: # @HELP build the source code
build:
	go build -v ./...

test: # @HELP run the unit tests and source code validation
test: build license #linters
	go test github.com/atomix/go-sdk/pkg/... -p 1

.PHONY: bench
bench:
	docker build . -t atomix/go-sdk-bench:latest -f bench/Dockerfile

kind: bench
	@if [ "`kind get clusters`" = '' ]; then echo "no kind cluster found" && exit 1; fi
	kind load docker-image atomix/go-sdk-bench:latest

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint
