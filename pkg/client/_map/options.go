package _map

import (
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
)

type PutOption interface {
	before(request *pb.PutRequest)
	after(response *pb.PutResponse)
}

func PutIfVersion(version int64) PutOption {
	return putIfVersionOption{version: version}
}

type putIfVersionOption struct {
	version int64
}

func (o putIfVersionOption) before(request *pb.PutRequest) {
	request.Version = o.version
}

func (o putIfVersionOption) after(response *pb.PutResponse) {

}

type GetOption interface {
	before(request *pb.GetRequest)
	after(response *pb.GetResponse)
}

func GetOrDefault(def []byte) GetOption {
	return getOrDefaultOption{def: def}
}

type getOrDefaultOption struct {
	def []byte
}

func (o getOrDefaultOption) before(request *pb.GetRequest) {
}

func (o getOrDefaultOption) after(response *pb.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}

type RemoveOption interface {
	before(request *pb.RemoveRequest)
	after(response *pb.RemoveResponse)
}

func RemoveIfVersion(version int64) RemoveOption {
	return removeIfVersionOption{version: version}
}

type removeIfVersionOption struct {
	version int64
}

func (o removeIfVersionOption) before(request *pb.RemoveRequest) {
	request.Version = o.version
}

func (o removeIfVersionOption) after(response *pb.RemoveResponse) {

}
