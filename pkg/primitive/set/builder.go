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

func NewBuilder[E any](client primitive.Client, name string) Builder[E] {
	return &setBuilder[E]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type setBuilder[E any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[E]
}

func (b *setBuilder[E]) Tag(tags ...string) Builder[E] {
	b.options.SetTags(tags...)
	return b
}

func (b *setBuilder[E]) Codec(codec types.Codec[E]) Builder[E] {
	b.codec = codec
	return b
}

func (b *setBuilder[E]) Get(ctx context.Context) (Set[E], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for set primitive")
	}

	client := setv1.NewSetsClient(conn)
	request := &setv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	response, err := client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	var set Set[string]
	config := response.Config
	set = newSetClient(b.options.Name, setv1.NewSetClient(conn))
	if config.Cache.Enabled {
		set, err = newCachingSet(ctx, set, int(config.Cache.Size_))
		if err != nil {
			return nil, err
		}
	}
	return newTranscodingSet[E](set, b.codec), nil
}

var _ Builder[any] = (*setBuilder[any])(nil)
