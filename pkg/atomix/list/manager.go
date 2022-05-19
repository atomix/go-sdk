// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &listManager{
		client: listv1.NewListManagerClient(conn),
	}
}

type listManager struct {
	client listv1.ListManagerClient
}

func (m *listManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &listv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *listManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &listv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*listManager)(nil)
