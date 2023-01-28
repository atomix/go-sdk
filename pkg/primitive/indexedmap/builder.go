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

func (b *Builder[K, V]) Get(ctx context.Context) (IndexedMap[K, V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for map primitive")
	}

	client := indexedmapv1.NewIndexedMapsClient(conn)
	request := &indexedmapv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	_, err = client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	closer := func(ctx context.Context) error {
		_, err := client.Close(ctx, &indexedmapv1.CloseRequest{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	return &indexedMapPrimitive[K, V]{
		Primitive:  primitive.New(b.options.Name, closer),
		client:     indexedmapv1.NewIndexedMapClient(conn),
		keyEncoder: scalar.NewEncodeFunc[K](),
		keyDecoder: scalar.NewDecodeFunc[K](),
		valueCodec: b.codec,
	}, nil
}

var _ primitive.Builder[*Builder[string, any], IndexedMap[string, any]] = (*Builder[string, any])(nil)
