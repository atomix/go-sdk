// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

type Builder[K scalar.Scalar, V any] interface {
	primitive.Builder[Builder[K, V], Map[K, V]]
	Codec(codec types.Codec[V]) Builder[K, V]
}

func NewBuilder[K scalar.Scalar, V any](client primitive.Client, name string) Builder[K, V] {
	return &mapBuilder[K, V]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type mapBuilder[K scalar.Scalar, V any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[V]
}

func (b *mapBuilder[K, V]) Tag(tags ...string) Builder[K, V] {
	b.options.SetTags(tags...)
	return b
}

func (b *mapBuilder[K, V]) Codec(codec types.Codec[V]) Builder[K, V] {
	b.codec = codec
	return b
}

func (b *mapBuilder[K, V]) Get(ctx context.Context) (Map[K, V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for map primitive")
	}

	client := mapv1.NewMapsClient(conn)
	request := &mapv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	response, err := client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	_map := newMapClient(b.options.Name, mapv1.NewMapClient(conn))
	config := response.Config
	if config.Cache.Enabled {
		_map, err = newCachingMap(ctx, _map, int(config.Cache.Size_))
		if err != nil {
			return nil, err
		}
	}
	return newTranscodingMap[K, V](_map, types.Scalar[K](), b.codec), nil
}

var _ Builder[string, any] = (*mapBuilder[string, any])(nil)
