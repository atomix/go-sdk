// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &lockManager{
		client: lockv1.NewLockManagerClient(conn),
	}
}

type lockManager struct {
	client lockv1.LockManagerClient
}

func (m *lockManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &lockv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *lockManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &lockv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*lockManager)(nil)
