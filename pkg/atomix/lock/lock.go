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
	api "github.com/atomix/atomix-api/go/atomix/primitive/lock/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-sdk-go/pkg/errors"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
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

// State is a lock state
type State int

const (
	// StateLocked is the State in which the lock is locked
	StateLocked State = iota
	// StateUnlocked is the State in which the lock is not locked
	StateUnlocked
)

// New creates a new Lock primitive for the given partitions
// The lock will be created in one of the given partitions.
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Lock, error) {
	options := newLockOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewLock(&options)
		}
	}
	sessions := api.NewLockSessionClient(conn)
	request := &api.OpenSessionRequest{
		Options: options.sessionOptions,
	}
	response, err := sessions.OpenSession(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &lock{
		Client:  primitive.NewClient(Type, name, response.SessionID),
		client:  api.NewLockClient(conn),
		session: sessions,
	}, nil
}

// lock is the single partition implementation of Lock
type lock struct {
	*primitive.Client
	client  api.LockClient
	session api.LockSessionClient
}

func (l *lock) Lock(ctx context.Context, opts ...LockOption) (Status, error) {
	request := &api.LockRequest{}
	for i := range opts {
		opts[i].beforeLock(request)
	}
	response, err := l.client.Lock(l.GetContext(ctx), request)
	if err != nil {
		return Status{}, errors.From(err)
	}
	for i := range opts {
		opts[i].afterLock(response)
	}
	var state State
	switch response.Lock.State {
	case api.LockInstance_LOCKED:
		state = StateLocked
	case api.LockInstance_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}

func (l *lock) Unlock(ctx context.Context, opts ...UnlockOption) error {
	request := &api.UnlockRequest{}
	for i := range opts {
		opts[i].beforeUnlock(request)
	}
	response, err := l.client.Unlock(l.GetContext(ctx), request)
	if err != nil {
		return errors.From(err)
	}
	for i := range opts {
		opts[i].afterUnlock(response)
	}
	return nil
}

func (l *lock) Get(ctx context.Context, opts ...GetOption) (Status, error) {
	request := &api.GetLockRequest{}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := l.client.GetLock(l.GetContext(ctx), request)
	if err != nil {
		return Status{}, errors.From(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	var state State
	switch response.Lock.State {
	case api.LockInstance_LOCKED:
		state = StateLocked
	case api.LockInstance_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}

func (l *lock) Close(ctx context.Context) error {
	request := &api.CloseSessionRequest{
		SessionID: l.SessionID(),
	}
	_, err := l.session.CloseSession(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}
