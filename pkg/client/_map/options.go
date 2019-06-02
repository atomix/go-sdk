package _map

import (
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
)

type PutOption interface {
	before(request *pb.PutRequest)
	after(response *pb.PutResponse)
}

func PutIfVersion(version int64) PutOption {
	return PutIfVersionOption{version: version}
}

type PutIfVersionOption struct {
	version int64
}

func (o PutIfVersionOption) before(request *pb.PutRequest) {
	request.Version = o.version
}

func (o PutIfVersionOption) after(response *pb.PutResponse) {

}

type GetOption interface {
	before(request *pb.GetRequest)
	after(response *pb.GetResponse)
}

func GetOrDefault(def []byte) GetOption {
	return GetOrDefaultOption{def: def}
}

type GetOrDefaultOption struct {
	def []byte
}

func (o GetOrDefaultOption) before(request *pb.GetRequest) {
}

func (o GetOrDefaultOption) after(response *pb.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}

type RemoveOption interface {
	before(request *pb.RemoveRequest)
	after(response *pb.RemoveResponse)
}

func RemoveIfVersion(version int64) RemoveOption {
	return RemoveIfVersionOption{version: version}
}

type RemoveIfVersionOption struct {
	version int64
}

func (o RemoveIfVersionOption) before(request *pb.RemoveRequest) {
	request.Version = o.version
}

func (o RemoveIfVersionOption) after(response *pb.RemoveResponse) {

}
