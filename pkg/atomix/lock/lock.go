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
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
)

// Type is the lock type
const Type primitive.Type = "Lock"

// Client provides an API for creating Locks
type Client interface {
	// GetLock gets the Lock instance of the given name
	GetLock(ctx context.Context, name string, opts ...Option) (Lock, error)
}

// Lock provides distributed concurrency control
type Lock interface {
	primitive.Primitive

	// Lock acquires the lock
	Lock(ctx context.Context, opts ...LockOption) (Status, error)

	// Unlock releases the lock
	Unlock(ctx context.Context, opts ...UnlockOption) error

	// Get gets the lock status
	Get(ctx context.Context, opts ...GetOption) (Status, error)
}

// Status is the lock status
type Status struct {
	meta.ObjectMeta
	State State
}

type State string

const StateLocked State = "locked"
const StateUnlocked State = "unlocked"

// New creates a new Lock primitive for the given partitions
// The lock will be created in one of the given partitions.
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Lock, error) {
	l := &lock{
		Client: primitive.NewClient(Type, name, conn),
		client: api.NewLockServiceClient(conn),
	}
	if err := l.Create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// lock is the single partition implementation of Lock
type lock struct {
	*primitive.Client
	client api.LockServiceClient
}

func (l *lock) Lock(ctx context.Context, opts ...LockOption) (Status, error) {
	request := &api.LockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeLock(request)
	}
	response, err := l.client.Lock(ctx, request)
	if err != nil {
		return Status{}, errors.From(err)
	}
	for i := range opts {
		opts[i].afterLock(response)
	}
	var state State
	switch response.Lock.State {
	case api.Lock_LOCKED:
		state = StateLocked
	case api.Lock_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}

func (l *lock) Unlock(ctx context.Context, opts ...UnlockOption) error {
	request := &api.UnlockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeUnlock(request)
	}
	response, err := l.client.Unlock(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	for i := range opts {
		opts[i].afterUnlock(response)
	}
	return nil
}

func (l *lock) Get(ctx context.Context, opts ...GetOption) (Status, error) {
	request := &api.GetLockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := l.client.GetLock(ctx, request)
	if err != nil {
		return Status{}, errors.From(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	var state State
	switch response.Lock.State {
	case api.Lock_LOCKED:
		state = StateLocked
	case api.Lock_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}
