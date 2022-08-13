// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/primitive"
	atomicvaluev1 "github.com/atomix/runtime/api/atomix/runtime/atomic/value/v1"
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
	codec   generic.Codec[V]
}

func (b *Builder[V]) Tag(key, value string) *Builder[V] {
	b.options.SetTag(key, value)
	return b
}

func (b *Builder[V]) Tags(tags map[string]string) *Builder[V] {
	b.options.SetTags(tags)
	return b
}

func (b *Builder[V]) Codec(codec generic.Codec[V]) *Builder[V] {
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
		client:    atomicvaluev1.NewAtomicValueClient(conn),
		codec:     b.codec,
	}
	if err := atomicValue.create(ctx, b.options.Tags); err != nil {
		return nil, err
	}
	return atomicValue, nil
}

var _ primitive.Builder[*Builder[any], Value[any]] = (*Builder[any])(nil)
