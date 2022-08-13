// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/generic/scalar"
	"github.com/atomix/go-client/pkg/primitive"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
)

func NewBuilder[K scalar.Scalar, V any](client primitive.Client, name string) *Builder[K, V] {
	return &Builder[K, V]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder[K scalar.Scalar, V any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   generic.Codec[V]
}

func (b *Builder[K, V]) Tag(key, value string) *Builder[K, V] {
	b.options.SetTag(key, value)
	return b
}

func (b *Builder[K, V]) Tags(tags map[string]string) *Builder[K, V] {
	b.options.SetTags(tags)
	return b
}

func (b *Builder[K, V]) Codec(codec generic.Codec[V]) *Builder[K, V] {
	b.codec = codec
	return b
}

func (b *Builder[K, V]) Get(ctx context.Context) (Map[K, V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for map primitive")
	}
	_map := &mapPrimitive[K, V]{
		Primitive:  primitive.New(b.options.Name),
		client:     mapv1.NewMapClient(conn),
		keyEncoder: scalar.NewEncodeFunc[K](),
		keyDecoder: scalar.NewDecodeFunc[K](),
		valueCodec: b.codec,
	}
	if err := _map.create(ctx, b.options.Tags); err != nil {
		return nil, err
	}
	return _map, nil
}

var _ primitive.Builder[*Builder[string, any], Map[string, any]] = (*Builder[string, any])(nil)
