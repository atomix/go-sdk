// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
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

func (b *Builder) Get(ctx context.Context) (Lock, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	counter := &lockPrimitive{
		Primitive: primitive.New(b.options.Name),
		client:    lockv1.NewLockClient(conn),
	}
	if err := counter.create(ctx, b.options.Tags...); err != nil {
		return nil, err
	}
	return counter, nil
}

var _ primitive.Builder[*Builder, Lock] = (*Builder)(nil)
