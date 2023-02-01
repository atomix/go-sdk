// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	atomicvaluev1 "github.com/atomix/atomix/api/runtime/value/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
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

	closer := func(ctx context.Context) error {
		_, err := client.Close(ctx, &valuev1.CloseRequest{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	var value Value[V] = &atomicValuePrimitive[V]{
		Primitive: primitive.New(b.options.Name, closer),
		client:    atomicvaluev1.NewValueClient(conn),
		codec:     b.codec,
	}

	config := response.Config
	if config.Cache.Enabled {
		value = newCachingValue[V](value)
	}
	return value, nil
}

var _ primitive.Builder[*Builder[any], Value[any]] = (*Builder[any])(nil)
