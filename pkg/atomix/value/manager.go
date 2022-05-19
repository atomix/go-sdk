// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &valueManager{
		client: valuev1.NewValueManagerClient(conn),
	}
}

type valueManager struct {
	client valuev1.ValueManagerClient
}

func (m *valueManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &valuev1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *valueManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &valuev1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*valueManager)(nil)
