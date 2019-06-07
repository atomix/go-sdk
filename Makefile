.PHONY: build

all: build

build:
	docker build -t atomix/atomix-go-build:0.1 build/proto
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-client atomix/atomix-go-build:0.1 build/proto
