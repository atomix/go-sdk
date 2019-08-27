// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lock

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

// Type is the lock type
const Type primitive.Type = "lock"

// Client provides an API for creating Locks
type Client interface {
	// GetLock gets the Lock instance of the given name
	GetLock(ctx context.Context, name string, opts ...session.Option) (Lock, error)
}

// Lock provides distributed concurrency control
type Lock interface {
	primitive.Primitive

	// Lock acquires the lock
	Lock(ctx context.Context, opts ...LockOption) (uint64, error)

	// Unlock releases the lock
	Unlock(ctx context.Context, opts ...UnlockOption) (bool, error)

	// IsLocked returns a bool indicating whether the lock is held
	IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error)
}

// New creates a new Lock primitive for the given partitions
// The lock will be created in one of the given partitions.
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (Lock, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newLock(ctx, name, partitions[i], opts...)
}

// newLock creates a new Lock primitive for the given partition
func newLock(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.Option) (*lock, error) {
	client := api.NewLockServiceClient(conn)
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &lock{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

// lock is the single partition implementation of Lock
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
		opts[i].beforeUnlock(request)
	}

	response, err := l.client.Unlock(ctx, request)
	if err != nil {
		return false, err
	}

	for i := range opts {
		opts[i].afterUnlock(response)
	}

	l.session.RecordResponse(request.Header, response.Header)
	return response.Unlocked, nil
}

func (l *lock) IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error) {
	request := &api.IsLockedRequest{
		Header: l.session.GetRequest(),
	}

	for i := range opts {
		opts[i].beforeIsLocked(request)
	}

	response, err := l.client.IsLocked(ctx, request)
	if err != nil {
		return false, err
	}

	for i := range opts {
		opts[i].afterIsLocked(response)
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
