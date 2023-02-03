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

func newLocksClient(name string, client lockv1.LocksClient) primitive.Primitive {
	return &locksClient{
		name:   name,
		client: client,
	}
}

type locksClient struct {
	name   string
	client lockv1.LocksClient
}

func (s *locksClient) Name() string {
	return s.name
}

func (s *locksClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &lockv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newLockClient(name string, client lockv1.LockClient) Lock {
	return &lockClient{
		Primitive: newLocksClient(name, client),
		client:    client,
	}
}

// lockClient is the single partition implementation of Lock
type lockClient struct {
	primitive.Primitive
	client lockv1.LockClient
}

func (l *lockClient) Lock(ctx context.Context, opts ...LockOption) (Version, error) {
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

func (l *lockClient) Unlock(ctx context.Context, opts ...UnlockOption) error {
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

func (l *lockClient) Get(ctx context.Context, opts ...GetOption) (Version, error) {
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
