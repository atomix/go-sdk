FROM golang:1.12

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    mkdir -p /go/src/github.com/google && \
    git clone --branch master https://github.com/google/protobuf /go/src/github.com/google/protobuf

RUN mkdir -p /go/src/github.com/atomix/atomix-go-client

WORKDIR "/go/src/github.com/atomix/atomix-go-client"

ENTRYPOINT ["./build/compile_protos.sh"]
