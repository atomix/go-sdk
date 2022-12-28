// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	atomicvaluev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
)

func NewBuilder[V any](client primitive.Client, name string) *Builder[V] {
	return &Builder[V]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder[V any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[V]
}

func (b *Builder[V]) Tag(tags ...string) *Builder[V] {
	b.options.SetTags(tags...)
	return b
}

func (b *Builder[V]) Codec(codec types.Codec[V]) *Builder[V] {
	b.codec = codec
	return b
}

func (b *Builder[V]) Get(ctx context.Context) (Value[V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for map primitive")
	}
	atomicValue := &atomicValuePrimitive[V]{
		Primitive: primitive.New(b.options.Name),
		client:    atomicvaluev1.NewValueClient(conn),
		codec:     b.codec,
	}
	if err := atomicValue.create(ctx, b.options.Tags...); err != nil {
		return nil, err
	}
	return atomicValue, nil
}

var _ primitive.Builder[*Builder[any], Value[any]] = (*Builder[any])(nil)
