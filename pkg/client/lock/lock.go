package lock

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

type LockClient interface {
	GetLock(ctx context.Context, name string, opts ...session.SessionOption) (Lock, error)
}

type Lock interface {
	primitive.Primitive
	Lock(ctx context.Context, opts ...LockOption) (uint64, error)
	Unlock(ctx context.Context, opts ...UnlockOption) (bool, error)
	IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error)
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Lock, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newLock(ctx, name, partitions[i], opts...)
}

func newLock(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.SessionOption) (*lock, error) {
	client := api.NewLockServiceClient(conn)
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &lock{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type lock struct {
	name    primitive.Name
	client  api.LockServiceClient
	session *session.Session
}

func (l *lock) Name() primitive.Name {
	return l.name
}

func (l *lock) Lock(ctx context.Context, opts ...LockOption) (uint64, error) {
	request := &api.LockRequest{
		Header: l.session.NextRequest(),
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

	l.session.RecordResponse(request.Header, response.Header)
	return response.Version, nil
}

func (l *lock) Unlock(ctx context.Context, opts ...UnlockOption) (bool, error) {
	request := &api.UnlockRequest{
		Header: l.session.NextRequest(),
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

	l.session.RecordResponse(request.Header, response.Header)
	return response.Unlocked, nil
}

func (l *lock) IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error) {
	request := &api.IsLockedRequest{
		Header: l.session.GetRequest(),
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

	l.session.RecordResponse(request.Header, response.Header)
	return response.IsLocked, nil
}

func (l *lock) Close() error {
	return l.session.Close()
}

func (l *lock) Delete() error {
	return l.session.Delete()
}
