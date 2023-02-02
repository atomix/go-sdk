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

	closer := func(ctx context.Context) error {
		_, err := client.Close(ctx, &mapv1.CloseRequest{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	var _map Map[K, V]
	_map = &mapPrimitive[K, V]{
		Primitive:  primitive.New(b.options.Name, closer),
		client:     mapv1.NewMapClient(conn),
		keyEncoder: scalar.NewEncodeFunc[K](),
		keyDecoder: scalar.NewDecodeFunc[K](),
		valueCodec: b.codec,
	}

	config := response.Config
	if config.Cache.Enabled {
		_map, err = newCachingMap[K, V](ctx, _map, int(config.Cache.Size_))
		if err != nil {
			return nil, err
		}
	}
	return _map, nil
}

var _ primitive.Builder[*Builder[string, any], Map[string, any]] = (*Builder[string, any])(nil)
