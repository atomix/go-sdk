// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/go-client/pkg/primitive"
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/lock/v1"
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

func (b *Builder) Tag(key, value string) *Builder {
	b.options.SetTag(key, value)
	return b
}

func (b *Builder) Tags(tags map[string]string) *Builder {
	b.options.SetTags(tags)
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
	if err := counter.create(ctx, b.options.Tags); err != nil {
		return nil, err
	}
	return counter, nil
}

var _ primitive.Builder[*Builder, Lock] = (*Builder)(nil)
