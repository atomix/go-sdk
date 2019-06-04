package lock

import (
	pb "github.com/atomix/atomix-go-client/proto/atomix/lock"
	"github.com/golang/protobuf/ptypes/duration"
	"time"
)

type LockOption interface {
	before(request *pb.LockRequest)
	after(response *pb.LockResponse)
}

func WithTimeout(timeout time.Duration) LockOption {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) before(request *pb.LockRequest) {
	request.Timeout = &duration.Duration{
		Seconds: int64(o.timeout.Seconds()),
		Nanos:   int32(o.timeout.Nanoseconds()),
	}
}

func (o timeoutOption) after(response *pb.LockResponse) {

}

type UnlockOption interface {
	before(request *pb.UnlockRequest)
	after(response *pb.UnlockResponse)
}

func WithVersion(version uint64) UnlockOption {
	return versionOption{version: version}
}

type versionOption struct {
	version uint64
}

func (o versionOption) before(request *pb.UnlockRequest) {
	request.Version = o.version
}

func (o versionOption) after(response *pb.UnlockResponse) {

}

type IsLockedOption interface {
	before(request *pb.IsLockedRequest)
	after(response *pb.IsLockedResponse)
}

func WithIsVersion(version uint64) IsLockedOption {
	return isVersionOption{version: version}
}

type isVersionOption struct {
	version uint64
}

func (o isVersionOption) before(request *pb.IsLockedRequest) {
	request.Version = o.version
}

func (o isVersionOption) after(response *pb.IsLockedResponse) {

}
