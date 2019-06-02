package lock

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	pb "github.com/atomix/atomix-go-client/proto/atomix/lock"
	"google.golang.org/grpc"
)

func NewLock(namespace string, name string, partitions []*grpc.ClientConn, opts ...session.Option) (*Lock, error) {
	i, err := util.GetPartitionIndex(name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newLock(namespace, name, partitions[i], opts...)
}

func newLock(namespace string, name string, conn *grpc.ClientConn, opts ...session.Option) (*Lock, error) {
	client := pb.NewLockServiceClient(conn)
	session := session.NewSession(namespace, name, &SessionHandler{client: client}, opts...)
	if err := session.Start(); err != nil {
		return nil, err
	}

	return &Lock{
		client:  client,
		session: session,
	}, nil
}

type Lock struct {
	Interface
	client  pb.LockServiceClient
	session *session.Session
}

func (l *Lock) Lock(ctx context.Context, opts ...LockOption) (uint64, error) {
	request := &pb.LockRequest{
		Header: l.session.NextHeader(),
	}

	for _, opt := range opts {
		opt.before(request)
	}

	response, err := l.client.Lock(ctx, request)
	if err != nil {
		return 0, err
	}

	for _, opt := range opts {
		opt.after(response)
	}

	l.session.UpdateHeader(response.Header)
	return response.Version, nil
}

func (l *Lock) Unlock(ctx context.Context, opts ...UnlockOption) (bool, error) {
	request := &pb.UnlockRequest{
		Header: l.session.NextHeader(),
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

	l.session.UpdateHeader(response.Header)
	return response.Unlocked, nil
}

func (l *Lock) IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error) {
	request := &pb.IsLockedRequest{
		Header: l.session.GetHeader(),
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

	l.session.UpdateHeader(response.Header)
	return response.IsLocked, nil
}

func (l *Lock) Close() error {
	return l.session.Stop()
}
