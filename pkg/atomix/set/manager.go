// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &setManager{
		client: setv1.NewSetManagerClient(conn),
	}
}

type setManager struct {
	client setv1.SetManagerClient
}

func (m *setManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &setv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *setManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &setv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*setManager)(nil)
