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

func NewBuilder(client primitive.Client, name string) *Builder {
	return &Builder{
		options: primitive.NewOptions(name),
		client:  client,
	}
}

type Builder struct {
	options *primitive.Options
	client  primitive.Client
}

func (b *Builder) Tag(tags ...string) *Builder {
	b.options.SetTags(tags...)
	return b
}

func (b *Builder) Get(ctx context.Context) (Lock, error) {
	conn, err := b.client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	client := lockv1.NewLockClient(conn)
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

	closer := func(ctx context.Context) error {
		_, err := client.Close(ctx, &lockv1.CloseRequest{})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return nil
	}

	return &lockPrimitive{
		Primitive: primitive.New(b.options.Name, closer),
		client:    lockv1.NewLockClient(conn),
	}, nil
}

var _ primitive.Builder[*Builder, Lock] = (*Builder)(nil)
