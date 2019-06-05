export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=0

.PHONY: build

all: build

proto:
	docker build -t atomix/atomix-go-build:0.1 build/proto
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-client atomix/atomix-go-build:0.1 build/proto

build:
	go build -o build/cli/_output/bin/atomix-cli ./cmd/cli
	docker build . -f build/cli/Dockerfile -t atomix/atomix-cli:latest