// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &mapManager{
		client: mapv1.NewMapManagerClient(conn),
	}
}

type mapManager struct {
	client mapv1.MapManagerClient
}

func (m *mapManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &mapv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *mapManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &mapv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*mapManager)(nil)
