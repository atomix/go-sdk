// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &counterManager{
		client: counterv1.NewCounterManagerClient(conn),
	}
}

type counterManager struct {
	client counterv1.CounterManagerClient
}

func (m *counterManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &counterv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *counterManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &counterv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*counterManager)(nil)
