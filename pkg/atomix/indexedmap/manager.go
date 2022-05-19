// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &indexedMapManager{
		client: indexedmapv1.NewIndexedMapManagerClient(conn),
	}
}

type indexedMapManager struct {
	client indexedmapv1.IndexedMapManagerClient
}

func (m *indexedMapManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &indexedmapv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *indexedMapManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &indexedmapv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*indexedMapManager)(nil)
