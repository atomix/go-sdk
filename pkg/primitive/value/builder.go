// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
)

func NewBuilder[V any](client primitive.Client, name string) Builder[V] {
	return &valueBuilder[V]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type valueBuilder[V any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[V]
}

func (b *valueBuilder[V]) Tag(tags ...string) Builder[V] {
	b.options.SetTags(tags...)
	return b
}

func (b *valueBuilder[V]) Codec(codec types.Codec[V]) Builder[V] {
	b.codec = codec
	return b
}

func (b *valueBuilder[V]) Get(ctx context.Context) (Value[V], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for map primitive")
	}

	client := valuev1.NewValuesClient(conn)
	request := &valuev1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	response, err := client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	var value Value[[]byte]
	config := response.Config
	value = newValueClient(b.options.Name, valuev1.NewValueClient(conn))
	if config.Cache.Enabled {
		value, err = newCachingValue(value)
		if err != nil {
			return nil, err
		}
	}
	return newTranscodingValue[V](value, b.codec), nil
}

var _ Builder[any] = (*valueBuilder[any])(nil)
