package lock

import (
	"context"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/session"
	pb "github.com/atomix/atomix-go/proto/lock"
	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/grpc"
	"time"
)

func NewLock(conn *grpc.ClientConn, name string, protocol *protocol.Protocol, opts ...session.Option) (*Lock, error) {
	c := pb.NewLockServiceClient(conn)
	s := newSession(c, name, protocol, opts...)
	if err := s.Connect(); err != nil {
		return nil, err
	}

	return &Lock{
		client: c,
		session: s,
	}, nil
}

type Lock struct {
	client  pb.LockServiceClient
	session *Session
}

func (l *Lock) Lock(ctx context.Context, timeout time.Duration) (uint64, error) {
	request := &pb.LockRequest{
		Id: l.session.lockId,
		Headers: l.session.Headers.Command(),
		Timeout: &duration.Duration{
			Seconds: int64(timeout.Seconds()),
			Nanos: int32(timeout.Nanoseconds()),
		},
	}

	response, err := l.client.Lock(ctx, request)
	if err != nil {
		return 0, err
	}
	l.session.Headers.Update(response.Headers)
	return response.Version, nil
}

func (l *Lock) Unlock(ctx context.Context, opts ...UnlockOption) (bool, error) {
	request := &pb.UnlockRequest{
		Id: l.session.lockId,
		Headers: l.session.Headers.Command(),
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := l.client.Unlock(ctx, request)
	if err != nil {
		return false, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	l.session.Headers.Update(response.Headers)
	return response.Unlocked, nil
}

type UnlockOption interface {
	before(request *pb.UnlockRequest)
	after(response *pb.UnlockResponse)
}

func UnlockIfVersion(version uint64) UnlockOption {
	return UnlockIfVersionOption{version: version}
}

type UnlockIfVersionOption struct {
	version uint64
}

func (o UnlockIfVersionOption) before(request *pb.UnlockRequest) {
	request.Version = o.version
}

func (o UnlockIfVersionOption) after(response *pb.UnlockResponse) {

}

func (l *Lock) IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error) {
	request := &pb.IsLockedRequest{
		Id: l.session.lockId,
		Headers: l.session.Headers.Query(),
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := l.client.IsLocked(ctx, request)
	if err != nil {
		return false, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	l.session.Headers.Update(response.Headers)
	return response.IsLocked, nil
}

type IsLockedOption interface {
	before(request *pb.IsLockedRequest)
	after(response *pb.IsLockedResponse)
}

func IsLockedVersion(version uint64) IsLockedOption {
	return IsLockedVersionOption{version: version}
}

type IsLockedVersionOption struct {
	version uint64
}

func (o IsLockedVersionOption) before(request *pb.IsLockedRequest) {
	request.Version = o.version
}

func (o IsLockedVersionOption) after(response *pb.IsLockedResponse) {

}

func (m *Lock) Close() error {
	return m.session.Close()
}
