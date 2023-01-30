// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
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
	codec   types.Codec[E]
}

func (b *Builder[E]) Tag(tags ...string) *Builder[E] {
	b.options.SetTags(tags...)
	return b
}

func (b *Builder[E]) Codec(codec types.Codec[E]) *Builder[E] {
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

	client := setv1.NewSetClient(conn)
	request := &setv1.CreateRequest{
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
		_, err := client.Close(ctx, &setv1.CloseRequest{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	return &setPrimitive[E]{
		Primitive: primitive.New(b.options.Name, closer),
		client:    setv1.NewSetClient(conn),
		codec:     b.codec,
	}, nil
}

var _ primitive.Builder[*Builder[any], Set[any]] = (*Builder[any])(nil)
