// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/list"
	"github.com/atomix/go-client/pkg/atomix/lock"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
	"google.golang.org/grpc"
)

func Counter(client *Client) primitive.Client[counter.Counter, counter.Option] {
	return newPrimitiveClient[counter.Counter, counter.Option](client, counter.Client)
}

func Election(client *Client) primitive.Client[election.Election, election.Option] {
	return newPrimitiveClient[election.Election, election.Option](client, election.Client)
}

func IndexedMap[K, V any](client *Client) primitive.Client[indexedmap.IndexedMap[K, V], indexedmap.Option[K, V]] {
	return newPrimitiveClient[indexedmap.IndexedMap[K, V], indexedmap.Option[K, V]](client, indexedmap.Client[K, V])
}

func List[E any](client *Client) primitive.Client[list.List[E], list.Option[E]] {
	return newPrimitiveClient[list.List[E], list.Option[E]](client, list.Client[E])
}

func Lock(client *Client) primitive.Client[lock.Lock, lock.Option] {
	return newPrimitiveClient[lock.Lock, lock.Option](client, lock.Client)
}

func Map[K, V any](client *Client) primitive.Client[_map.Map[K, V], _map.Option[K, V]] {
	return newPrimitiveClient[_map.Map[K, V], _map.Option[K, V]](client, _map.Client[K, V])
}

func Set[E any](client *Client) primitive.Client[set.Set[E], set.Option[E]] {
	return newPrimitiveClient[set.Set[E], set.Option[E]](client, set.Client[E])
}

func Value[E any](client *Client) primitive.Client[value.Value[E], value.Option[E]] {
	return newPrimitiveClient[value.Value[E], value.Option[E]](client, value.Client[E])
}

func newPrimitiveClient[T primitive.Primitive, O any](client *Client, factory func(conn *grpc.ClientConn) primitive.Client[T, O]) primitive.Client[T, O] {
	return &primitiveClient[T, O]{
		client:  client,
		factory: factory,
	}
}

type primitiveClient[T primitive.Primitive, O any] struct {
	client  *Client
	factory func(conn *grpc.ClientConn) primitive.Client[T, O]
}

func (c *primitiveClient[T, O]) Get(ctx context.Context, name string, opts ...primitive.Option) func(...O) (T, error) {
	return func(primitiveOpts ...O) (T, error) {
		conn, err := c.client.connect(ctx)
		if err != nil {
			return nil, err
		}
		return c.factory(conn).Get(ctx, name, opts...)(primitiveOpts...)
	}
}
