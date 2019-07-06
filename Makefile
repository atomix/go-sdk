.PHONY: build proto

all: build

build:
	go build ./...
proto:
	docker build -t atomix/atomix-go-build:0.2 build/proto
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-client atomix/atomix-go-build:0.2 build/proto
