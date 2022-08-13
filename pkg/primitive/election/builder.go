// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/go-client/pkg/primitive"
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
)

func NewBuilder(client primitive.Client, name string) *Builder {
	return &Builder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder struct {
	options     *primitive.Options
	client      primitive.Client
	candidateID string
}

func (b *Builder) Tag(key, value string) *Builder {
	b.options.SetTag(key, value)
	return b
}

func (b *Builder) Tags(tags map[string]string) *Builder {
	b.options.SetTags(tags)
	return b
}

func (b *Builder) CandidateID(candidateID string) *Builder {
	b.candidateID = candidateID
	return b
}

func (b *Builder) Get(ctx context.Context) (Election, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	election := &electionPrimitive{
		Primitive:   primitive.New(b.options.Name),
		client:      electionv1.NewLeaderElectionClient(conn),
		candidateID: b.candidateID,
	}
	if err := election.create(ctx, b.options.Tags); err != nil {
		return nil, err
	}
	return election, nil
}

var _ primitive.Builder[*Builder, Election] = (*Builder)(nil)
