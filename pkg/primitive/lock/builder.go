// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
)

func NewBuilder(client primitive.Client, name string) Builder {
	return &lockBuilder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type lockBuilder struct {
	options *primitive.Options
	client  primitive.Client
}

func (b *lockBuilder) Tag(tags ...string) Builder {
	b.options.SetTags(tags...)
	return b
}

func (b *lockBuilder) Get(ctx context.Context) (Lock, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	client := lockv1.NewLocksClient(conn)
	request := &lockv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: b.options.Name,
		},
		Tags: b.options.Tags,
	}
	_, err = client.Create(ctx, request)
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return newLockClient(b.options.Name, lockv1.NewLockClient(conn)), nil
}

var _ Builder = (*lockBuilder)(nil)
