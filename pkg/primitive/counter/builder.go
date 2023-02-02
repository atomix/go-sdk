// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
)

func NewBuilder(client primitive.Client, name string) Builder {
	return &counterBuilder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type counterBuilder struct {
	options *primitive.Options
	client  primitive.Client
}

func (b *counterBuilder) Tag(tags ...string) Builder {
	b.options.SetTags(tags...)
	return b
}

func (b *counterBuilder) Get(ctx context.Context) (Counter, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	client := counterv1.NewCountersClient(conn)
	request := &counterv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	_, err = client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return newCounterClient(b.options.Name, counterv1.NewCounterClient(conn)), nil
}

var _ Builder = (*counterBuilder)(nil)
