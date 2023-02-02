// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
)

func NewBuilder(client primitive.Client, name string) Builder {
	return &electionBuilder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type electionBuilder struct {
	options     *primitive.Options
	client      primitive.Client
	candidateID string
}

func (b *electionBuilder) Tag(tags ...string) Builder {
	b.options.SetTags(tags...)
	return b
}

func (b *electionBuilder) CandidateID(candidateID string) Builder {
	b.candidateID = candidateID
	return b
}

func (b *electionBuilder) Get(ctx context.Context) (Election, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	client := electionv1.NewLeaderElectionsClient(conn)
	request := &electionv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	_, err = client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return newElectionClient(b.options.Name, b.candidateID, electionv1.NewLeaderElectionClient(conn)), nil
}

var _ Builder = (*electionBuilder)(nil)
