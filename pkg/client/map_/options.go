package map_

import (
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
)

type PutOption interface {
	beforePut(request *pb.PutRequest)
	afterPut(response *pb.PutResponse)
}

type RemoveOption interface {
	beforeRemove(request *pb.RemoveRequest)
	afterRemove(response *pb.RemoveResponse)
}

func WithVersion(version int64) VersionOption {
	return VersionOption{version: version}
}

type VersionOption struct {
	PutOption
	RemoveOption
	version int64
}

func (o VersionOption) beforePut(request *pb.PutRequest) {
	request.Version = o.version
}

func (o VersionOption) afterPut(response *pb.PutResponse) {

}

func (o VersionOption) beforeRemove(request *pb.RemoveRequest) {
	request.Version = o.version
}

func (o VersionOption) afterRemove(response *pb.RemoveResponse) {

}

type GetOption interface {
	beforeGet(request *pb.GetRequest)
	afterGet(response *pb.GetResponse)
}

func WithDefault(def []byte) GetOption {
	return defaultOption{def: def}
}

type defaultOption struct {
	def []byte
}

func (o defaultOption) beforeGet(request *pb.GetRequest) {
}

func (o defaultOption) afterGet(response *pb.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}
