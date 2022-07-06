// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/time"
)

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
	State     State
	Timestamp time.Timestamp
}

// State is a lock state
type State int

const (
	// StateLocked is the State in which the lock is locked
	StateLocked State = iota
	// StateUnlocked is the State in which the lock is not locked
	StateUnlocked
)

func New(client lockv1.LockClient) func(context.Context, string, ...Option) (Lock, error) {
	return func(ctx context.Context, name string, opts ...Option) (Lock, error) {
		var options Options
		options.Apply(opts...)
		lock := &lockPrimitive{
			Primitive: primitive.New(name),
			client:    client,
		}
		if err := lock.create(ctx, options.Tags); err != nil {
			return nil, err
		}
		return lock, nil
	}
}

// lockPrimitive is the single partition implementation of Lock
type lockPrimitive struct {
	primitive.Primitive
	client lockv1.LockClient
}

func (l *lockPrimitive) Lock(ctx context.Context, opts ...LockOption) (Status, error) {
	request := &lockv1.LockRequest{
		ID: primitivev1.PrimitiveId{
			Name: l.Name(),
		},
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
		State:     state,
		Timestamp: time.NewTimestamp(*response.Lock.Timestamp),
	}, nil
}

func (l *lockPrimitive) Unlock(ctx context.Context, opts ...UnlockOption) error {
	request := &lockv1.UnlockRequest{
		ID: primitivev1.PrimitiveId{
			Name: l.Name(),
		},
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
		ID: primitivev1.PrimitiveId{
			Name: l.Name(),
		},
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
		State:     state,
		Timestamp: time.NewTimestamp(*response.Lock.Timestamp),
	}, nil
}

func (l *lockPrimitive) create(ctx context.Context, tags map[string]string) error {
	request := &lockv1.CreateRequest{
		ID: primitivev1.PrimitiveId{
			Name: l.Name(),
		},
		Tags: tags,
	}
	_, err := l.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (l *lockPrimitive) Close(ctx context.Context) error {
	request := &lockv1.CloseRequest{
		ID: primitivev1.PrimitiveId{
			Name: l.Name(),
		},
	}
	_, err := l.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
