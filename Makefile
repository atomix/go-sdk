.PHONY: build

all: build

build:
	docker build -t atomix/atomix-go-build:0.1 build
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go atomix/atomix-go-build:0.1 build