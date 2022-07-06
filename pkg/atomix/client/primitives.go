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
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

func GetCounter(client *Client) func(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
	return getPrimitive[counter.Counter, counter.Option](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...counter.Option) (counter.Counter, error) {
			return func(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
				return counter.New(counterv1.NewCounterClient(conn))(ctx, name, opts...)
			}
		})
}

func GetElection(client *Client) func(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return getPrimitive[election.Election, election.Option](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...election.Option) (election.Election, error) {
			return func(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
				return election.New(electionv1.NewLeaderElectionClient(conn))(ctx, name, opts...)
			}
		})
}

func GetIndexedMap[K, V any](client *Client) func(ctx context.Context, name string, opts ...indexedmap.Option[K, V]) (indexedmap.IndexedMap[K, V], error) {
	return getPrimitive[indexedmap.IndexedMap[K, V], indexedmap.Option[K, V]](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...indexedmap.Option[K, V]) (indexedmap.IndexedMap[K, V], error) {
			return func(ctx context.Context, name string, opts ...indexedmap.Option[K, V]) (indexedmap.IndexedMap[K, V], error) {
				return indexedmap.New[K, V](indexedmapv1.NewIndexedMapClient(conn))(ctx, name, opts...)
			}
		})
}

func GetList[E any](client *Client) func(ctx context.Context, name string, opts ...list.Option[E]) (list.List[E], error) {
	return getPrimitive[list.List[E], list.Option[E]](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...list.Option[E]) (list.List[E], error) {
			return func(ctx context.Context, name string, opts ...list.Option[E]) (list.List[E], error) {
				return list.New[E](listv1.NewListClient(conn))(ctx, name, opts...)
			}
		})
}

func GetLock(client *Client) func(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
	return getPrimitive[lock.Lock, lock.Option](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...lock.Option) (lock.Lock, error) {
			return func(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
				return lock.New(lockv1.NewLockClient(conn))(ctx, name, opts...)
			}
		})
}

func GetMap[K, V any](client *Client) func(ctx context.Context, name string, opts ..._map.Option[K, V]) (_map.Map[K, V], error) {
	return getPrimitive[_map.Map[K, V], _map.Option[K, V]](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ..._map.Option[K, V]) (_map.Map[K, V], error) {
			return func(ctx context.Context, name string, opts ..._map.Option[K, V]) (_map.Map[K, V], error) {
				return _map.New[K, V](mapv1.NewMapClient(conn))(ctx, name, opts...)
			}
		})
}

func GetSet[E any](client *Client) func(ctx context.Context, name string, opts ...set.Option[E]) (set.Set[E], error) {
	return getPrimitive[set.Set[E], set.Option[E]](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...set.Option[E]) (set.Set[E], error) {
			return func(ctx context.Context, name string, opts ...set.Option[E]) (set.Set[E], error) {
				return set.New[E](setv1.NewSetClient(conn))(ctx, name, opts...)
			}
		})
}

func GetValue[V any](client *Client) func(ctx context.Context, name string, opts ...value.Option[V]) (value.Value[V], error) {
	return getPrimitive[value.Value[V], value.Option[V]](client,
		func(conn *grpc.ClientConn) func(context.Context, string, ...value.Option[V]) (value.Value[V], error) {
			return func(ctx context.Context, name string, opts ...value.Option[V]) (value.Value[V], error) {
				return value.New[V](valuev1.NewValueClient(conn))(ctx, name, opts...)
			}
		})
}

func getPrimitive[P primitive.Primitive, O any](client *Client, f func(conn *grpc.ClientConn) func(context.Context, string, ...O) (P, error)) func(context.Context, string, ...O) (P, error) {
	return func(ctx context.Context, name string, opts ...O) (P, error) {
		var p P
		conn, err := client.connect(ctx)
		if err != nil {
			return p, errors.FromProto(err)
		}
		return f(conn)(ctx, name, opts...)
	}
}
