// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

// Type is the lock type
const Type primitive.Type = "Lock"

// Client provides an API for creating Locks
type Client interface {
	// GetLock gets the Lock instance of the given name
	GetLock(ctx context.Context, name string, opts ...primitive.Option) (Lock, error)
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
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Lock, error) {
	options := newLockOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewLock(&options)
		}
	}
	l := &lock{
		Client:  primitive.NewClient(Type, name, conn, opts...),
		client:  api.NewLockServiceClient(conn),
		options: options,
	}
	if err := l.Create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// lock is the single partition implementation of Lock
type lock struct {
	*primitive.Client
	client  api.LockServiceClient
	options newLockOptions
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
