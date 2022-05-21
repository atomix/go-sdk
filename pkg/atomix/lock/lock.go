// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/meta"
)

const serviceName = "atomix.lock.v1.Lock"

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

func Provider(client primitive.Client) primitive.Provider[Lock, Option] {
	return primitive.NewProvider[Lock, Option](func(ctx context.Context, name string, opts ...primitive.Option) func(...Option) (Lock, error) {
		return func(lockOpts ...Option) (Lock, error) {
			// Process the primitive options
			var options Options
			options.apply(lockOpts...)

			// Construct the primitive configuration
			var config lockv1.LockConfig

			// Open the primitive connection
			base, conn, err := primitive.Open[*lockv1.LockConfig](client)(ctx, serviceName, name, &config, opts...)
			if err != nil {
				return nil, err
			}

			// Create the primitive instance
			return &lockPrimitive{
				ManagedPrimitive: base,
				client:           lockv1.NewLockClient(conn),
			}, nil
		}
	})
}

// lockPrimitive is the single partition implementation of Lock
type lockPrimitive struct {
	*primitive.ManagedPrimitive
	client  lockv1.LockClient
	options newLockOptions
}

func (l *lockPrimitive) Lock(ctx context.Context, opts ...LockOption) (Status, error) {
	request := &lockv1.LockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeLock(request)
	}
	response, err := l.client.Lock(ctx, request)
	if err != nil {
		return Status{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterLock(response)
	}
	var state State
	switch response.Lock.State {
	case lockv1.LockInstance_LOCKED:
		state = StateLocked
	case lockv1.LockInstance_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}

func (l *lockPrimitive) Unlock(ctx context.Context, opts ...UnlockOption) error {
	request := &lockv1.UnlockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeUnlock(request)
	}
	response, err := l.client.Unlock(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterUnlock(response)
	}
	return nil
}

func (l *lockPrimitive) Get(ctx context.Context, opts ...GetOption) (Status, error) {
	request := &lockv1.GetLockRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := l.client.GetLock(ctx, request)
	if err != nil {
		return Status{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	var state State
	switch response.Lock.State {
	case lockv1.LockInstance_LOCKED:
		state = StateLocked
	case lockv1.LockInstance_UNLOCKED:
		state = StateUnlocked
	}
	return Status{
		ObjectMeta: meta.FromProto(response.Lock.ObjectMeta),
		State:      state,
	}, nil
}
