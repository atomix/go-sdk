// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
)

// Lock provides distributed concurrency control
type Lock interface {
	primitive.Primitive

	// Lock acquires the lock
	Lock(ctx context.Context, opts ...LockOption) (Version, error)

	// Unlock releases the lock
	Unlock(ctx context.Context, opts ...UnlockOption) error

	// Get gets the lock status
	Get(ctx context.Context, opts ...GetOption) (Version, error)
}

type Version uint64

// lockPrimitive is the single partition implementation of Lock
type lockPrimitive struct {
	primitive.Primitive
	client lockv1.LockClient
}

func (l *lockPrimitive) Lock(ctx context.Context, opts ...LockOption) (Version, error) {
	request := &lockv1.LockRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeLock(request)
	}
	response, err := l.client.Lock(ctx, request)
	if err != nil {
		return 0, err
	}
	for i := range opts {
		opts[i].afterLock(response)
	}
	return Version(response.Version), nil
}

func (l *lockPrimitive) Unlock(ctx context.Context, opts ...UnlockOption) error {
	request := &lockv1.UnlockRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeUnlock(request)
	}
	response, err := l.client.Unlock(ctx, request)
	if err != nil {
		return err
	}
	for i := range opts {
		opts[i].afterUnlock(response)
	}
	return nil
}

func (l *lockPrimitive) Get(ctx context.Context, opts ...GetOption) (Version, error) {
	request := &lockv1.GetLockRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := l.client.GetLock(ctx, request)
	if err != nil {
		return 0, err
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return Version(response.Version), nil
}

func (l *lockPrimitive) create(ctx context.Context, tags ...string) error {
	request := &lockv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Tags: tags,
	}
	_, err := l.client.Create(ctx, request)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (l *lockPrimitive) Close(ctx context.Context) error {
	request := &lockv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	_, err := l.client.Close(ctx, request)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
