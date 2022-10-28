// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-sdk/pkg/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
)

func NewBuilder(client primitive.Client, name string) *Builder {
	return &Builder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder struct {
	options *primitive.Options
	client  primitive.Client
}

func (b *Builder) Tag(tags ...string) *Builder {
	b.options.SetTags(tags...)
	return b
}

func (b *Builder) Get(ctx context.Context) (AtomicCounter, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	counter := &counterPrimitive{
		Primitive: primitive.New(b.options.Name),
		client:    counterv1.NewCounterClient(conn),
	}
	if err := counter.create(ctx, b.options.Tags...); err != nil {
		return nil, err
	}
	return counter, nil
}

var _ primitive.Builder[*Builder, AtomicCounter] = (*Builder)(nil)
