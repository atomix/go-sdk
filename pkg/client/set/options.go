package set

import (
	pb "github.com/atomix/atomix-go-client/proto/atomix/set"
)

type WatchOption interface {
	beforeWatch(request *pb.EventRequest)
	afterWatch(response *pb.EventResponse)
}

// WithReplay returns a Watch option to replay entries
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *pb.EventRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *pb.EventResponse) {

}
