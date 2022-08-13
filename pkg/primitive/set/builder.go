// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/primitive"
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
)

func NewBuilder[E any](client primitive.Client, name string) *Builder[E] {
	return &Builder[E]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder[E any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   generic.Codec[E]
}

func (b *Builder[E]) Tag(key, value string) *Builder[E] {
	b.options.SetTag(key, value)
	return b
}

func (b *Builder[E]) Tags(tags map[string]string) *Builder[E] {
	b.options.SetTags(tags)
	return b
}

func (b *Builder[E]) Codec(codec generic.Codec[E]) *Builder[E] {
	b.codec = codec
	return b
}

func (b *Builder[E]) Get(ctx context.Context) (Set[E], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for set primitive")
	}
	set := &setPrimitive[E]{
		Primitive: primitive.New(b.options.Name),
		client:    setv1.NewSetClient(conn),
		codec:     b.codec,
	}
	if err := set.create(ctx, b.options.Tags); err != nil {
		return nil, err
	}
	return set, nil
}

var _ primitive.Builder[*Builder[any], Set[any]] = (*Builder[any])(nil)
