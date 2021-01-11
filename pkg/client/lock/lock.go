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
	api "github.com/atomix/api/go/atomix/primitive/lock"
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	lockclient "github.com/atomix/go-framework/pkg/atomix/client/lock"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(lockclient.PrimitiveType)

// Client provides an API for creating Locks
type Client interface {
	// GetLock gets the Lock instance of the given name
	GetLock(ctx context.Context, name string, opts ...Option) (Lock, error)
}

// Lock provides distributed concurrency control
type Lock interface {
	primitive.Primitive

	// Lock acquires the lock
	Lock(ctx context.Context, opts ...LockOption) (meta.ObjectMeta, error)

	// Unlock releases the lock
	Unlock(ctx context.Context, opts ...UnlockOption) (bool, error)

	// IsLocked returns a bool indicating whether the lock is held
	IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error)
}

// New creates a new Lock primitive for the given partitions
// The lock will be created in one of the given partitions.
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Lock, error) {
	options := applyOptions(opts...)
	l := &lock{
		client: lockclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := l.create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// lock is the single partition implementation of Lock
type lock struct {
	client lockclient.Client
}

func (l *lock) Type() primitive.Type {
	return Type
}

func (l *lock) Name() string {
	return l.client.Name()
}

func (l *lock) Lock(ctx context.Context, opts ...LockOption) (meta.ObjectMeta, error) {
	input := &api.LockInput{
	}
	for i := range opts {
		opts[i].beforeLock(input)
	}
	output, err := l.client.Lock(ctx, input)
	if err != nil {
		return meta.ObjectMeta{}, err
	}
	for i := range opts {
		opts[i].afterLock(output)
	}
	return meta.New(output.Meta), nil
}

func (l *lock) Unlock(ctx context.Context, opts ...UnlockOption) (bool, error) {
	input := &api.UnlockInput{
	}
	for i := range opts {
		opts[i].beforeUnlock(input)
	}
	output, err := l.client.Unlock(ctx, input)
	if err != nil {
		return false, err
	}
	for i := range opts {
		opts[i].afterUnlock(output)
	}
	return output.Unlocked, nil
}

func (l *lock) IsLocked(ctx context.Context, opts ...IsLockedOption) (bool, error) {
	input := &api.IsLockedInput{
	}
	for i := range opts {
		opts[i].beforeIsLocked(input)
	}
	output, err := l.client.IsLocked(ctx, input)
	if err != nil {
		return false, err
	}
	for i := range opts {
		opts[i].afterIsLocked(output)
	}
	return output.IsLocked, nil
}

func (l *lock) create(ctx context.Context) error {
	return l.client.Create(ctx)
}

func (l *lock) Close(ctx context.Context) error {
	return l.client.Close(ctx)
}

func (l *lock) Delete(ctx context.Context) error {
	return l.client.Delete(ctx)
}
