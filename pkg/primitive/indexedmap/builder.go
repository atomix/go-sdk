// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

func NewBuilder[K scalar.Scalar, V any](client primitive.Client, name string) Builder[K, V] {
	return &indexedMapBuilder[K, V]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type indexedMapBuilder[K scalar.Scalar, V any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[V]
}

func (b *indexedMapBuilder[K, V]) Tag(tags ...string) Builder[K, V] {
	b.options.SetTags(tags...)
	return b
}

func (b *indexedMapBuilder[K, V]) Codec(codec types.Codec[V]) Builder[K, V] {
	b.codec = codec
	return b
}

func (b *indexedMapBuilder[K, V]) Get(ctx context.Context) (IndexedMap[K, V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for IndexedMap primitive")
	}

	client := indexedmapv1.NewIndexedMapsClient(conn)
	request := &indexedmapv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	response, err := client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	_map := newIndexedMapClient(b.options.Name, indexedmapv1.NewIndexedMapClient(conn))
	config := response.Config
	if config.Cache.Enabled {
		_map, err = newCachingIndexedMap(ctx, _map, int(config.Cache.Size_))
		if err != nil {
			return nil, err
		}
	}
	return newTranscodingIndexedMap[K, V](_map, types.Scalar[K](), b.codec), nil
}

var _ Builder[string, any] = (*indexedMapBuilder[string, any])(nil)
