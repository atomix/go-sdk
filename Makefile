export CGO_ENABLED=0

.PHONY: build

all: build

build:
	./build/compile_protos.sh