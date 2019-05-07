#!/bin/sh

rm -rf build/_output/atomix
rm -rf proto
git clone --branch simple-raft-primitives https://github.com/atomix/atomix.git build/_output/atomix
mv build/_output/atomix/grpc/src/main/proto proto

proto_imports="./proto:${GOPATH}/src/github.com/google/protobuf/src:${GOPATH}/src"

protoc -I=$proto_imports --go_out=import_path=cluster,plugins=grpc:proto proto/cluster/*.proto
protoc -I=$proto_imports --go_out=import_path=events,plugins=grpc:proto proto/events/*.proto
protoc -I=$proto_imports --go_out=import_path=headers,plugins=grpc:proto proto/headers/*.proto
protoc -I=$proto_imports --go_out=plugins=grpc:proto proto/protocol/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,import_path=partitions,plugins=grpc:proto proto/partitions/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,import_path=primitives,plugins=grpc:proto proto/primitives/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=counter,plugins=grpc:proto proto/counter/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=lock,plugins=grpc:proto proto/lock/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=log,plugins=grpc:proto proto/log/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=map,plugins=grpc:proto proto/map/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=set,plugins=grpc:proto proto/set/*.proto
protoc -I=$proto_imports --go_out=Mprotocol/protocol.proto=github.com/atomix/atomix-go/proto/protocol,Mheaders/headers.proto=github.com/atomix/atomix-go/proto/headers,import_path=value,plugins=grpc:proto proto/value/*.proto