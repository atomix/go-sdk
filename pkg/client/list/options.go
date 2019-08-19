package list

import (
	api "github.com/atomix/atomix-api/proto/atomix/list"
)

type WatchOption interface {
	beforeWatch(request *api.EventRequest)
	afterWatch(response *api.EventResponse)
}

// WithReplay returns a Watch option to replay entries
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventResponse) {

}
