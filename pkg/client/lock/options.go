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

func LockTimeout(timeout time.Duration) LockOption {
	return lockTimeoutOption{timeout: timeout}
}

type lockTimeoutOption struct {
	timeout time.Duration
}

func (o lockTimeoutOption) before(request *pb.LockRequest) {
	request.Timeout = &duration.Duration{
		Seconds: int64(o.timeout.Seconds()),
		Nanos:   int32(o.timeout.Nanoseconds()),
	}
}

func (o lockTimeoutOption) after(response *pb.LockResponse) {

}

type UnlockOption interface {
	before(request *pb.UnlockRequest)
	after(response *pb.UnlockResponse)
}

func UnlockVersion(version uint64) UnlockOption {
	return unlockVersionOption{version: version}
}

type unlockVersionOption struct {
	version uint64
}

func (o unlockVersionOption) before(request *pb.UnlockRequest) {
	request.Version = o.version
}

func (o unlockVersionOption) after(response *pb.UnlockResponse) {

}

type IsLockedOption interface {
	before(request *pb.IsLockedRequest)
	after(response *pb.IsLockedResponse)
}

func IsLockedVersion(version uint64) IsLockedOption {
	return isLockedVersionOption{version: version}
}

type isLockedVersionOption struct {
	version uint64
}

func (o isLockedVersionOption) before(request *pb.IsLockedRequest) {
	request.Version = o.version
}

func (o isLockedVersionOption) after(response *pb.IsLockedResponse) {

}
