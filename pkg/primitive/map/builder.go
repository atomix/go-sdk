// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
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
	codec   types.Codec[V]
}

func (b *Builder[K, V]) Tag(tags ...string) *Builder[K, V] {
	b.options.SetTags(tags...)
	return b
}

func (b *Builder[K, V]) Codec(codec types.Codec[V]) *Builder[K, V] {
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
	atomicMap := &mapPrimitive[K, V]{
		Primitive:  primitive.New(b.options.Name),
		client:     mapv1.NewMapClient(conn),
		keyEncoder: scalar.NewEncodeFunc[K](),
		keyDecoder: scalar.NewDecodeFunc[K](),
		valueCodec: b.codec,
	}
	if err := atomicMap.create(ctx, b.options.Tags...); err != nil {
		return nil, err
	}
	return atomicMap, nil
}

var _ primitive.Builder[*Builder[string, any], Map[string, any]] = (*Builder[string, any])(nil)
