// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func newManager(conn *grpc.ClientConn) primitive.Manager {
	return &electionManager{
		client: electionv1.NewLeaderElectionManagerClient(conn),
	}
}

type electionManager struct {
	client electionv1.LeaderElectionManagerClient
}

func (m *electionManager) Create(ctx context.Context, primitive runtimev1.Primitive) error {
	request := &electionv1.CreateRequest{
		Primitive: primitive,
	}
	if _, err := m.client.Create(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *electionManager) Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error {
	request := &electionv1.CloseRequest{
		PrimitiveID: primitiveID,
	}
	if _, err := m.client.Close(ctx, request); err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ primitive.Manager = (*electionManager)(nil)
