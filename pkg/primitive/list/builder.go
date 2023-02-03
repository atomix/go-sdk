// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types"
)

func NewBuilder[E any](client primitive.Client, name string) Builder[E] {
	return &listBuilder[E]{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type listBuilder[E any] struct {
	options *primitive.Options
	client  primitive.Client
	codec   types.Codec[E]
}

func (b *listBuilder[E]) Tag(tags ...string) Builder[E] {
	b.options.SetTags(tags...)
	return b
}

func (b *listBuilder[E]) Codec(codec types.Codec[E]) Builder[E] {
	b.codec = codec
	return b
}

func (b *listBuilder[E]) Get(ctx context.Context) (List[E], error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if b.codec == nil {
		panic("no codec set for list primitive")
	}

	client := listv1.NewListsClient(conn)
	request := &listv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	_, err = client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}

	var list List[[]byte]
	list = newListClient(b.options.Name, listv1.NewListClient(conn))
	return newTranscodingList[E](list, b.codec), nil
}

var _ Builder[any] = (*listBuilder[any])(nil)
