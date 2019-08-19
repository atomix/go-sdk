package _map

import (
	api "github.com/atomix/atomix-api/proto/atomix/map"
)

type PutOption interface {
	beforePut(request *api.PutRequest)
	afterPut(response *api.PutResponse)
}

type RemoveOption interface {
	beforeRemove(request *api.RemoveRequest)
	afterRemove(response *api.RemoveResponse)
}

func WithVersion(version int64) VersionOption {
	return VersionOption{version: version}
}

type VersionOption struct {
	PutOption
	RemoveOption
	version int64
}

func (o VersionOption) beforePut(request *api.PutRequest) {
	request.Version = o.version
}

func (o VersionOption) afterPut(response *api.PutResponse) {

}

func (o VersionOption) beforeRemove(request *api.RemoveRequest) {
	request.Version = o.version
}

func (o VersionOption) afterRemove(response *api.RemoveResponse) {

}

type GetOption interface {
	beforeGet(request *api.GetRequest)
	afterGet(response *api.GetResponse)
}

func WithDefault(def []byte) GetOption {
	return defaultOption{def: def}
}

type defaultOption struct {
	def []byte
}

func (o defaultOption) beforeGet(request *api.GetRequest) {
}

func (o defaultOption) afterGet(response *api.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}

// WatchOption is an option for a map Watch request
type WatchOption interface {
	beforeWatch(request *api.EventRequest)
	afterWatch(response *api.EventResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventResponse) {

}
